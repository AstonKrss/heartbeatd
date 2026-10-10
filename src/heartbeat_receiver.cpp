#include "heartbeat_receiver.h"

#include "server_log.h"
#include "thread_affinity.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace heartbeat {
namespace detail {
namespace {

using SteadyClock = std::chrono::steady_clock;

// 每次 recvmmsg 最多收 64 个数据报。相比 32，高吞吐下系统调用次数减半；
// 低速时多设置几个未使用的 msghdr 也只是数百个 store，可以忽略。
constexpr unsigned int kReceiveBatchSize = 64;
constexpr std::size_t kMaximumEpollEvents = 1;
constexpr int kEpollWaitMilliseconds = 100;

class FileDescriptor {
public:
    explicit FileDescriptor(int value = -1) : value_(value) {}
    ~FileDescriptor() { reset(); }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    int get() const { return value_; }
    void reset(int value = -1) {
        if (value_ >= 0) {
            close(value_);
        }
        value_ = value;
    }

private:
    int value_;
};

struct ReceiveSlot {
    std::array<std::uint8_t, kMaximumDatagramSize> data{};
    sockaddr_in source{};
    iovec data_vector{};
    alignas(cmsghdr) std::array<char, CMSG_SPACE(sizeof(std::uint32_t))> control{};
};

bool is_allowed_source(const sockaddr_in& source,
                       const std::vector<in_addr>& allowed_sources) {
    if (allowed_sources.empty()) {
        return true;
    }
    for (const in_addr& allowed_source : allowed_sources) {
        if (source.sin_addr.s_addr == allowed_source.s_addr) {
            return true;
        }
    }
    return false;
}

bool add_socket_to_epoll(int epoll_fd, int socket_fd) {
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = socket_fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &event) == 0;
}

// SO_RXQ_OVFL 通过控制消息报告 socket 接收队列的丢包累计值；
// 两次读数之差就是从上次统计以来内核丢掉的包数。
void update_kernel_drop_counter(msghdr& message_header,
                                std::uint32_t* last_drop_count,
                                bool* has_drop_count,
                                ReceiverMetrics* metrics) {
    for (cmsghdr* control = CMSG_FIRSTHDR(&message_header);
         control != nullptr;
         control = CMSG_NXTHDR(&message_header, control)) {
        if (control->cmsg_level != SOL_SOCKET || control->cmsg_type != SO_RXQ_OVFL ||
            control->cmsg_len < CMSG_LEN(sizeof(std::uint32_t))) {
            continue;
        }

        std::uint32_t current_drop_count = 0;
        std::memcpy(&current_drop_count, CMSG_DATA(control), sizeof(current_drop_count));
        const std::uint32_t previous_drop_count = *has_drop_count ? *last_drop_count : 0;
        const std::uint32_t delta = current_drop_count - previous_drop_count;
        if (delta > 0) {
            metrics->kernel_dropped_datagrams.fetch_add(delta, std::memory_order_relaxed);
        }
        *last_drop_count = current_drop_count;
        *has_drop_count = true;
        return;
    }
}
}  // namespace

Receiver::Receiver(std::size_t index,
                   int socket_fd,
                   int actual_receive_buffer,
                   bool kernel_drop_counter_enabled,
                   const std::vector<in_addr>* allowed_sources,
                   std::vector<std::unique_ptr<Worker>>* workers,
                   std::atomic<bool>* stopping,
                   std::atomic<bool>* failed,
                   int cpu_pin)
    : index_(index),
      socket_fd_(socket_fd),
      allowed_sources_(allowed_sources),
      workers_(workers),
      stopping_(stopping),
      failed_(failed),
      cpu_pin_(cpu_pin) {
    metrics_.actual_receive_buffer = actual_receive_buffer;
    metrics_.kernel_drop_counter_enabled = kernel_drop_counter_enabled;
}
Receiver::~Receiver() {
    if (thread_.joinable()) {
        stopping_->store(true, std::memory_order_relaxed);
        thread_.join();
    }
}

void Receiver::start() {
    thread_ = std::thread(&Receiver::run_guarded, this);
}

const ReceiverMetrics& Receiver::metrics() const {
    return metrics_;
}

void Receiver::join() {
    if (thread_.joinable()) {
        thread_.join();
    }
}

void Receiver::run_guarded() {
    try {
        run();
    } catch (const std::exception& exception) {
        log_line("receiver=" + std::to_string(index_) +
                 " failed: " + exception.what());
        failed_->store(true, std::memory_order_relaxed);
        stopping_->store(true, std::memory_order_relaxed);
    } catch (...) {
        log_line("receiver=" + std::to_string(index_) + " failed: unknown error");
        failed_->store(true, std::memory_order_relaxed);
        stopping_->store(true, std::memory_order_relaxed);
    }
}

void Receiver::run() {
    if (cpu_pin_ >= 0) {
        std::string pin_error;
        if (!pin_current_thread_to_cpu(cpu_pin_, &pin_error)) {
            log_line("warning receiver=" + std::to_string(index_) +
                     " could not pin to cpu=" + std::to_string(cpu_pin_) +
                     " error=" + pin_error);
        }
    }

    FileDescriptor epoll_fd(epoll_create1(EPOLL_CLOEXEC));
    if (epoll_fd.get() < 0) {
        log_line("receiver=" + std::to_string(index_) +
                 " epoll_create1 failed: " + std::strerror(errno));
        failed_->store(true, std::memory_order_relaxed);
        stopping_->store(true, std::memory_order_relaxed);
        return;
    }
    if (!add_socket_to_epoll(epoll_fd.get(), socket_fd_)) {
        log_line("receiver=" + std::to_string(index_) +
                 " epoll_ctl failed: " + std::strerror(errno));
        failed_->store(true, std::memory_order_relaxed);
        stopping_->store(true, std::memory_order_relaxed);
        return;
    }

    std::array<ReceiveSlot, kReceiveBatchSize> receive_slots{};
    std::array<mmsghdr, kReceiveBatchSize> receive_messages{};
    for (ReceiveSlot& slot : receive_slots) {
        slot.data_vector.iov_base = slot.data.data();
        slot.data_vector.iov_len = slot.data.size();
    }
    // 每个 worker 一个待投递批次；clear 后容量保留，稳态不会重新分配。
    std::vector<std::vector<QueuedHeartbeat>> dispatch_batches(workers_->size());
    for (std::vector<QueuedHeartbeat>& batch : dispatch_batches) {
        batch.reserve(kReceiveBatchSize);
    }
    std::uint32_t last_kernel_drop_count = 0;
    bool has_kernel_drop_count = false;
    std::string decode_error;
    std::array<epoll_event, kMaximumEpollEvents> events{};

    while (!stopping_->load(std::memory_order_relaxed)) {
        const int event_count = epoll_wait(
            epoll_fd.get(), events.data(), static_cast<int>(events.size()),
            kEpollWaitMilliseconds);
        if (event_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            log_line("receiver=" + std::to_string(index_) +
                     " epoll_wait failed: " + std::strerror(errno));
            failed_->store(true, std::memory_order_relaxed);
            stopping_->store(true, std::memory_order_relaxed);
            return;
        }
        if (event_count == 0) {
            continue;
        }

        while (!stopping_->load(std::memory_order_relaxed)) {
            for (std::size_t index = 0; index < kReceiveBatchSize; ++index) {
                receive_messages[index] = {};
                ReceiveSlot& slot = receive_slots[index];
                mmsghdr& message = receive_messages[index];
                message.msg_hdr.msg_name = &slot.source;
                message.msg_hdr.msg_namelen = sizeof(slot.source);
                message.msg_hdr.msg_iov = &slot.data_vector;
                message.msg_hdr.msg_iovlen = 1;
                message.msg_hdr.msg_control = slot.control.data();
                message.msg_hdr.msg_controllen = slot.control.size();
            }

            const int received_count = recvmmsg(
                socket_fd_, receive_messages.data(), kReceiveBatchSize,
                MSG_DONTWAIT, nullptr);
            if (received_count < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                if (errno == EINTR) {
                    continue;
                }
                log_line("receiver=" + std::to_string(index_) +
                         " recvmmsg failed: " + std::strerror(errno));
                failed_->store(true, std::memory_order_relaxed);
                stopping_->store(true, std::memory_order_relaxed);
                return;
            }
            if (received_count == 0) {
                break;
            }

            metrics_.datagrams_received.fetch_add(
                static_cast<std::uint64_t>(received_count), std::memory_order_relaxed);

            std::uint64_t valid_in_batch = 0;
            std::uint64_t malformed_in_batch = 0;
            std::uint64_t unauthorized_in_batch = 0;

            for (std::vector<QueuedHeartbeat>& batch : dispatch_batches) {
                batch.clear();
            }

            for (int message_index = 0; message_index < received_count; ++message_index) {
                mmsghdr& received_message = receive_messages[message_index];
                update_kernel_drop_counter(received_message.msg_hdr,
                                           &last_kernel_drop_count,
                                           &has_kernel_drop_count,
                                           &metrics_);

                const SteadyClock::time_point received_at = SteadyClock::now();
                if (!is_allowed_source(receive_slots[message_index].source,
                                       *allowed_sources_)) {
                    ++unauthorized_in_batch;
                    continue;
                }
                if ((received_message.msg_hdr.msg_flags & MSG_TRUNC) != 0) {
                    ++malformed_in_batch;
                    continue;
                }

                HeartbeatMessage heartbeat;
                if (!decode_heartbeat(receive_slots[message_index].data.data(),
                                      received_message.msg_len,
                                      &heartbeat, &decode_error)) {
                    ++malformed_in_batch;
                    continue;
                }

                ++valid_in_batch;
                const std::size_t worker_index =
                    std::hash<std::string>{}(heartbeat.host_id) % workers_->size();
                dispatch_batches[worker_index].push_back(
                    QueuedHeartbeat{std::move(heartbeat), received_at});
            }

            metrics_.valid_heartbeats.fetch_add(valid_in_batch, std::memory_order_relaxed);
            metrics_.malformed_datagrams.fetch_add(malformed_in_batch, std::memory_order_relaxed);
            metrics_.unauthorized_datagrams.fetch_add(unauthorized_in_batch, std::memory_order_relaxed);

            for (std::size_t index = 0; index < dispatch_batches.size(); ++index) {
                if (dispatch_batches[index].empty()) {
                    continue;
                }
                (*workers_)[index]->enqueue_batch(&dispatch_batches[index]);
            }
        }
    }
}

}  // namespace detail
}  // namespace heartbeat