#include "admin_socket.h"
#include "heartbeat/heartbeat_server.h"
#include "heartbeat/protocol.h"
#include "heartbeat_worker.h"
#include "heartbeat_receiver.h"
#include "server_log.h"
#include "thread_affinity.h"

#include <arpa/inet.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <vector>
namespace heartbeat {
namespace {

using SteadyClock = std::chrono::steady_clock;
constexpr int kMaximumEpollEvents = 4;
constexpr unsigned int kReceiveBatchSize = 32;
constexpr auto kStatisticsInterval = std::chrono::seconds(10);

class FileDescriptor {
public:
    explicit FileDescriptor(int value = -1) : value_(value) {}
    ~FileDescriptor() { reset(); }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    int get() const { return value_; }
    explicit operator bool() const { return value_ >= 0; }

    void reset(int value = -1) {
        if (value_ >= 0) {
            close(value_);
        }
        value_ = value;
    }

private:
    int value_;
};

using detail::Counters;
using detail::QueuedHeartbeat;
using detail::Receiver;
using detail::ReceiverMetrics;
using detail::Worker;
using detail::WorkerMetrics;
using detail::log_line;
bool add_to_epoll(int epoll_fd, int target_fd, std::uint32_t events) {
    epoll_event event{};
    event.events = events;
    event.data.fd = target_fd;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, target_fd, &event) == 0;
}

bool try_bind_socket(const HeartbeatServerConfig& config,
                     bool use_reuse_port,
                     FileDescriptor* socket_fd,
                     int* actual_receive_buffer,
                     bool* kernel_drop_counter_enabled,
                     std::string* error) {
    const int raw_fd = socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (raw_fd < 0) {
        *error = std::string("socket failed: ") + std::strerror(errno);
        return false;
    }
    socket_fd->reset(raw_fd);

    if (use_reuse_port) {
        const int enable_reuse_port = 1;
        if (setsockopt(socket_fd->get(), SOL_SOCKET, SO_REUSEPORT,
                       &enable_reuse_port, sizeof(enable_reuse_port)) < 0) {
            *error = std::string("setsockopt(SO_REUSEPORT) failed: ") + std::strerror(errno);
            return false;
        }
    }

    if (setsockopt(socket_fd->get(), SOL_SOCKET, SO_RCVBUF,
                   &config.receive_buffer_bytes, sizeof(config.receive_buffer_bytes)) < 0) {
        *error = std::string("setsockopt(SO_RCVBUF) failed: ") + std::strerror(errno);
        return false;
    }
    socklen_t receive_buffer_length = sizeof(*actual_receive_buffer);
    if (getsockopt(socket_fd->get(), SOL_SOCKET, SO_RCVBUF,
                   actual_receive_buffer, &receive_buffer_length) < 0) {
        *error = std::string("getsockopt(SO_RCVBUF) failed: ") + std::strerror(errno);
        return false;
    }

    const int enable_drop_counter = 1;
    *kernel_drop_counter_enabled =
        setsockopt(socket_fd->get(), SOL_SOCKET, SO_RXQ_OVFL,
                   &enable_drop_counter, sizeof(enable_drop_counter)) == 0;

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(config.port);
    if (inet_pton(AF_INET, config.bind_address.c_str(), &address.sin_addr) != 1) {
        *error = "bind address must be an IPv4 literal";
        return false;
    }
    if (bind(socket_fd->get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        *error = std::string("bind failed: ") + std::strerror(errno);
        return false;
    }
    return true;
}
struct ReceiverTotals {
    std::uint64_t valid = 0;
    std::uint64_t malformed = 0;
    std::uint64_t unauthorized = 0;
    std::uint64_t kernel_dropped = 0;
};

// 直方图快照格式化为一行文本；饱和时在 max 后追加 "+"，表示真实值更大。
std::string format_latency(const detail::LatencyHistogram::Summary& latency) {
    std::string text = "latency_samples=" + std::to_string(latency.sample_count) +
                       " latency_p50_us=" + std::to_string(latency.p50_us) +
                       " latency_p99_us=" + std::to_string(latency.p99_us) +
                       " latency_max_us=" + std::to_string(latency.max_us);
    if (latency.saturated) {
        text += "+";
    }
    return text;
}

ReceiverTotals collect_receiver_totals(
    const std::vector<std::unique_ptr<Receiver>>& receivers) {
    ReceiverTotals totals;
    for (const std::unique_ptr<Receiver>& receiver : receivers) {
        const ReceiverMetrics& metrics = receiver->metrics();
        totals.valid += metrics.valid_heartbeats.load(std::memory_order_relaxed);
        totals.malformed += metrics.malformed_datagrams.load(std::memory_order_relaxed);
        totals.unauthorized += metrics.unauthorized_datagrams.load(std::memory_order_relaxed);
        totals.kernel_dropped += metrics.kernel_dropped_datagrams.load(std::memory_order_relaxed);
    }
    return totals;
}
void print_statistics(const Counters& counters,
                      const std::vector<std::unique_ptr<Worker>>& workers,
                      const std::vector<std::unique_ptr<Receiver>>& receivers) {
    const ReceiverTotals receiver_totals = collect_receiver_totals(receivers);
    log_line("stats active_hosts=" +
             std::to_string(counters.active_hosts.load(std::memory_order_relaxed)) +
             " valid=" +
             std::to_string(receiver_totals.valid) +
             " malformed=" +
             std::to_string(receiver_totals.malformed) +
             " unauthorized=" +
             std::to_string(receiver_totals.unauthorized) +
             " kernel_dropped=" +
             std::to_string(receiver_totals.kernel_dropped) +
             " queue_dropped=" +
             std::to_string(counters.queue_dropped.load(std::memory_order_relaxed)) +
             " capacity_rejected=" +
             std::to_string(counters.capacity_rejected.load(std::memory_order_relaxed)) +
             " stale=" +
             std::to_string(counters.stale_heartbeats.load(std::memory_order_relaxed)) +
             " online_events=" +
             std::to_string(counters.online_events.load(std::memory_order_relaxed)) +
             " offline_events=" +
             std::to_string(counters.offline_events.load(std::memory_order_relaxed)));

    for (std::size_t index = 0; index < workers.size(); ++index) {
        const WorkerMetrics& metrics = workers[index]->metrics();
        log_line("shard=" + std::to_string(index) +
                 " active_hosts=" +
                 std::to_string(metrics.active_hosts.load(std::memory_order_relaxed)) +
                 " queue_depth=" +
                 std::to_string(metrics.queue_depth.load(std::memory_order_relaxed)) +
                 " queue_high_water=" +
                 std::to_string(metrics.queue_high_water.load(std::memory_order_relaxed)) +
                 " processed=" +
                 std::to_string(metrics.processed_heartbeats.load(std::memory_order_relaxed)) +
                 " queue_dropped=" +
                 std::to_string(metrics.queue_dropped.load(std::memory_order_relaxed)) +
                 " " + format_latency(metrics.latency.summarize()));
    }

    for (std::size_t index = 0; index < receivers.size(); ++index) {
        const ReceiverMetrics& metrics = receivers[index]->metrics();
        log_line("receiver=" + std::to_string(index) +
                 " buffer_actual=" + std::to_string(metrics.actual_receive_buffer) +
                 " rxq_counter=" + (metrics.kernel_drop_counter_enabled ? "on" : "off") +
                 " received=" +
                 std::to_string(metrics.datagrams_received.load(std::memory_order_relaxed)) +
                 " valid=" +
                 std::to_string(metrics.valid_heartbeats.load(std::memory_order_relaxed)) +
                 " malformed=" +
                 std::to_string(metrics.malformed_datagrams.load(std::memory_order_relaxed)) +
                 " unauthorized=" +
                 std::to_string(metrics.unauthorized_datagrams.load(std::memory_order_relaxed)) +
                 " kernel_dropped=" +
                 std::to_string(metrics.kernel_dropped_datagrams.load(std::memory_order_relaxed)));
    }
}

}

namespace {

std::string handle_admin_command(
    const std::string& command,
    const Counters& counters,
    const std::vector<std::unique_ptr<Worker>>& workers,
    const std::vector<std::unique_ptr<Receiver>>& receivers) {
    std::istringstream input(command);
    std::string operation;
    input >> operation;

    if (operation == "STATS") {
        std::string extra;
        if (input >> extra) {
            return "ERR STATS takes no arguments\n";
        }

        const ReceiverTotals receiver_totals = collect_receiver_totals(receivers);
        std::ostringstream output;
        output << "OK\n"
               << "active_hosts " << counters.active_hosts.load(std::memory_order_relaxed) << '\n'
               << "valid " << receiver_totals.valid << '\n'
               << "malformed " << receiver_totals.malformed << '\n'
               << "unauthorized " << receiver_totals.unauthorized << '\n'
               << "kernel_dropped " << receiver_totals.kernel_dropped << '\n'
               << "queue_dropped " << counters.queue_dropped.load(std::memory_order_relaxed) << '\n'
               << "capacity_rejected " << counters.capacity_rejected.load(std::memory_order_relaxed) << '\n'
               << "stale " << counters.stale_heartbeats.load(std::memory_order_relaxed) << '\n'
               << "online_events " << counters.online_events.load(std::memory_order_relaxed) << '\n'
               << "offline_events " << counters.offline_events.load(std::memory_order_relaxed) << '\n';
        for (std::size_t index = 0; index < workers.size(); ++index) {
            const WorkerMetrics& metrics = workers[index]->metrics();
            output << "shard " << index
                   << " active_hosts=" << metrics.active_hosts.load(std::memory_order_relaxed)
                   << " queue_depth=" << metrics.queue_depth.load(std::memory_order_relaxed)
                   << " queue_high_water=" << metrics.queue_high_water.load(std::memory_order_relaxed)
                   << " processed=" << metrics.processed_heartbeats.load(std::memory_order_relaxed)
                   << " queue_dropped=" << metrics.queue_dropped.load(std::memory_order_relaxed)
                   << " " << format_latency(metrics.latency.summarize())
                   << '\n';
        }
        for (std::size_t index = 0; index < receivers.size(); ++index) {
            const ReceiverMetrics& metrics = receivers[index]->metrics();
            output << "receiver " << index
                   << " buffer_actual=" << metrics.actual_receive_buffer
                   << " received=" << metrics.datagrams_received.load(std::memory_order_relaxed)
                   << " valid=" << metrics.valid_heartbeats.load(std::memory_order_relaxed)
                   << " malformed=" << metrics.malformed_datagrams.load(std::memory_order_relaxed)
                   << " unauthorized=" << metrics.unauthorized_datagrams.load(std::memory_order_relaxed)
                   << " kernel_dropped=" << metrics.kernel_dropped_datagrams.load(std::memory_order_relaxed)
                   << '\n';
        }
        return output.str();
    }

    if (operation == "RECEIVER") {
        std::string extra;
        if (input >> extra) {
            return "ERR RECEIVER takes no arguments\n";
        }
        std::ostringstream output;
        output << "OK\n";
        for (std::size_t index = 0; index < receivers.size(); ++index) {
            const ReceiverMetrics& metrics = receivers[index]->metrics();
            output << "receiver " << index
                   << " buffer_actual=" << metrics.actual_receive_buffer
                   << " received=" << metrics.datagrams_received.load(std::memory_order_relaxed)
                   << " valid=" << metrics.valid_heartbeats.load(std::memory_order_relaxed)
                   << " malformed=" << metrics.malformed_datagrams.load(std::memory_order_relaxed)
                   << " unauthorized=" << metrics.unauthorized_datagrams.load(std::memory_order_relaxed)
                   << " kernel_dropped=" << metrics.kernel_dropped_datagrams.load(std::memory_order_relaxed)
                   << '\n';
        }
        return output.str();
    }

    if (operation != "LIST") {
        return "ERR supported commands: STATS, RECEIVER, LIST [limit]\n";
    }

    std::size_t limit = 1000;
    std::string limit_text;
    if (input >> limit_text) {
        std::uint64_t parsed_limit = 0;
        const auto parse_result = std::from_chars(
            limit_text.data(), limit_text.data() + limit_text.size(), parsed_limit);
        if (parse_result.ec != std::errc{} ||
            parse_result.ptr != limit_text.data() + limit_text.size() ||
            parsed_limit == 0 || parsed_limit > 10000) {
            return "ERR LIST limit must be between 1 and 10000\n";
        }
        limit = static_cast<std::size_t>(parsed_limit);
    }
    std::string extra;
    if (input >> extra) {
        return "ERR LIST accepts at most one limit\n";
    }

    std::vector<std::size_t> worker_limits(workers.size(), 0);
    std::uint64_t remaining_hosts = 0;
    for (const std::unique_ptr<Worker>& worker : workers) {
        remaining_hosts += worker->metrics().active_hosts.load(std::memory_order_relaxed);
    }

    std::size_t remaining_limit = limit;
    for (std::size_t index = 0; index < workers.size(); ++index) {
        const std::size_t shard_hosts =
            workers[index]->metrics().active_hosts.load(std::memory_order_relaxed);
        std::size_t shard_limit = 0;
        if (shard_hosts > 0 && remaining_limit > 0) {
            if (remaining_hosts <= remaining_limit) {
                shard_limit = std::min(shard_hosts, remaining_limit);
            } else {
                const std::uint64_t weighted_limit =
                    (static_cast<std::uint64_t>(remaining_limit) * shard_hosts +
                     remaining_hosts - 1) / remaining_hosts;
                shard_limit = std::min(
                    remaining_limit,
                    std::min(shard_hosts, static_cast<std::size_t>(weighted_limit)));
            }
        }
        worker_limits[index] = shard_limit;
        remaining_limit -= shard_limit;
        remaining_hosts -= shard_hosts;
    }

    std::vector<std::future<HostStatusSnapshot>> pending_snapshots;
    pending_snapshots.reserve(workers.size());
    for (std::size_t index = 0; index < workers.size(); ++index) {
        pending_snapshots.push_back(
            workers[index]->request_snapshot(worker_limits[index]));
    }

    std::vector<HostStatusEntry> hosts;
    hosts.reserve(limit);
    bool truncated = false;
    for (std::future<HostStatusSnapshot>& pending : pending_snapshots) {
        HostStatusSnapshot snapshot = pending.get();
        truncated = truncated || snapshot.truncated;
        for (HostStatusEntry& host : snapshot.hosts) {
            hosts.push_back(std::move(host));
        }
    }

    std::sort(hosts.begin(), hosts.end(), [](const HostStatusEntry& left,
                                             const HostStatusEntry& right) {
        return left.host_id < right.host_id;
    });
    if (hosts.size() > limit) {
        hosts.resize(limit);
        truncated = true;
    }

    std::ostringstream output;
    output << "OK " << hosts.size() << " truncated=" << (truncated ? 1 : 0) << '\n';
    for (const HostStatusEntry& host : hosts) {
        output << host.host_id << ' ' << host.last_seen_ago_ms << '\n';
    }
    return output.str();
}
}  // namespace

int run_heartbeat_server(const HeartbeatServerConfig& config) {
    if (config.port == 0 || config.worker_count == 0 || config.worker_count > 256 ||
        config.receiver_count == 0 || config.receiver_count > 64 ||
        config.queue_capacity_per_worker == 0 || config.maximum_hosts == 0 ||
        config.maximum_hosts > 1000000 || config.timeout_ms < 100 ||
        config.timeout_ms > 86400000 || config.receive_buffer_bytes < 65536 ||
        config.receive_buffer_bytes > 67108864 ||
        config.queue_capacity_per_worker > 1000000 / config.worker_count) {
        log_line("invalid server configuration or total queue capacity exceeds 1000000 packets");
        return 2;
    }

    std::vector<in_addr> allowed_sources;
    allowed_sources.reserve(config.allowed_sources.size());
    for (const std::string& source_text : config.allowed_sources) {
        in_addr source{};
        if (inet_pton(AF_INET, source_text.c_str(), &source) != 1) {
            log_line("invalid allowed source IPv4 address: " + source_text);
            return 2;
        }
        allowed_sources.push_back(source);
    }

    sigset_t signal_mask;
    sigemptyset(&signal_mask);
    sigaddset(&signal_mask, SIGINT);
    sigaddset(&signal_mask, SIGTERM);
    const int signal_result = pthread_sigmask(SIG_BLOCK, &signal_mask, nullptr);
    if (signal_result != 0) {
        log_line(std::string("failed to block signals: ") + std::strerror(signal_result));
        return 1;
    }

    std::vector<std::unique_ptr<FileDescriptor>> udp_sockets;
    std::vector<int> actual_receive_buffers;
    std::vector<bool> receiver_drop_counter_enabled;
    udp_sockets.reserve(config.receiver_count);
    actual_receive_buffers.reserve(config.receiver_count);
    receiver_drop_counter_enabled.reserve(config.receiver_count);
    bool all_kernel_drop_counters_enabled = true;
    std::string error;

    for (std::size_t index = 0; index < config.receiver_count; ++index) {
        auto socket_fd = std::make_unique<FileDescriptor>();
        int actual_receive_buffer = 0;
        bool kernel_drop_counter_enabled = false;
        if (!try_bind_socket(config, config.receiver_count > 1, socket_fd.get(),
                             &actual_receive_buffer, &kernel_drop_counter_enabled,
                             &error)) {
            log_line("receiver=" + std::to_string(index) + " " + error);
            return 1;
        }
        udp_sockets.push_back(std::move(socket_fd));
        actual_receive_buffers.push_back(actual_receive_buffer);
        receiver_drop_counter_enabled.push_back(kernel_drop_counter_enabled);
        all_kernel_drop_counters_enabled =
            all_kernel_drop_counters_enabled && kernel_drop_counter_enabled;
    }

    const int minimum_receive_buffer = *std::min_element(
        actual_receive_buffers.begin(), actual_receive_buffers.end());
    const int maximum_receive_buffer = *std::max_element(
        actual_receive_buffers.begin(), actual_receive_buffers.end());

    FileDescriptor epoll_fd(epoll_create1(EPOLL_CLOEXEC));
    if (epoll_fd.get() < 0) {
        log_line(std::string("epoll_create1 failed: ") + std::strerror(errno));
        return 1;
    }
    FileDescriptor signal_fd(signalfd(-1, &signal_mask, SFD_NONBLOCK | SFD_CLOEXEC));
    if (signal_fd.get() < 0) {
        log_line(std::string("signalfd failed: ") + std::strerror(errno));
        return 1;
    }
    if (!add_to_epoll(epoll_fd.get(), signal_fd.get(), EPOLLIN)) {
        log_line(std::string("epoll_ctl failed: ") + std::strerror(errno));
        return 1;
    }

    // CPU 绑定规划：receivers 从 cpu_base 开始，workers 紧随其后。
    // 绑定失败不是致命错误，各线程只记录告警并继续。
    const int worker_cpu_base = config.cpu_base >= 0
        ? config.cpu_base + static_cast<int>(config.receiver_count)
        : -1;
    if (config.cpu_base >= 0) {
        const int available_cpus = detail::available_cpu_count();
        const std::size_t pinned_threads = config.receiver_count + config.worker_count;
        if (available_cpus > 0 && static_cast<std::size_t>(available_cpus) < pinned_threads) {
            log_line("warning cpu_base=" + std::to_string(config.cpu_base) +
                     " wants to pin " + std::to_string(pinned_threads) +
                     " threads but only " + std::to_string(available_cpus) +
                     " CPUs are available; per-thread pin failures will be logged");
        }
    }

    Counters counters;
    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(config.worker_count);
    const auto timeout = std::chrono::milliseconds(config.timeout_ms);
    const std::size_t expected_hosts_per_worker =
        (config.maximum_hosts + config.worker_count - 1) / config.worker_count;
    try {
        for (std::size_t index = 0; index < config.worker_count; ++index) {
            const int worker_cpu_pin =
                worker_cpu_base >= 0 ? worker_cpu_base + static_cast<int>(index) : -1;
            auto worker = std::make_unique<Worker>(
                index, config.queue_capacity_per_worker, timeout,
                config.maximum_hosts, expected_hosts_per_worker,
                &counters, config.log_host_events, worker_cpu_pin);
            worker->start();
            workers.push_back(std::move(worker));
        }
    } catch (const std::exception& exception) {
        log_line(std::string("could not start worker threads: ") + exception.what());
        return 1;
    }

    std::atomic<bool> stop_receivers{false};
    std::atomic<bool> receiver_failed{false};
    std::vector<std::unique_ptr<Receiver>> receivers;
    receivers.reserve(config.receiver_count);
    try {
        for (std::size_t index = 0; index < config.receiver_count; ++index) {
            const int receiver_cpu_pin = config.cpu_base >= 0
                ? config.cpu_base + static_cast<int>(index)
                : -1;
            receivers.push_back(std::make_unique<Receiver>(
                index, udp_sockets[index]->get(), actual_receive_buffers[index],
                receiver_drop_counter_enabled[index], &allowed_sources,
                &workers, &stop_receivers, &receiver_failed, receiver_cpu_pin));
        }
    } catch (const std::exception& exception) {
        log_line(std::string("could not create receiver contexts: ") + exception.what());
        return 1;
    }

    AdminSocketServer admin_server;
    if (!config.admin_socket_path.empty()) {
        if (!admin_server.start(
                config.admin_socket_path,
                [&counters, &workers, &receivers](const std::string& command) {
                    return handle_admin_command(command, counters, workers, receivers);
                },
                &error)) {
            log_line(error);
            return 1;
        }
        log_line("admin socket ready path=" + config.admin_socket_path);
    }

    log_line("listening address=" + config.bind_address +
             " port=" + std::to_string(config.port) +
             " workers=" + std::to_string(config.worker_count) +
             " receivers=" + std::to_string(config.receiver_count) +
             " reuseport=" + (config.receiver_count > 1 ? "on" : "off") +
             " timeout_ms=" + std::to_string(config.timeout_ms) +
             " max_hosts=" + std::to_string(config.maximum_hosts) +
             " receive_buffer_requested=" + std::to_string(config.receive_buffer_bytes) +
             " receive_buffer_actual_min=" + std::to_string(minimum_receive_buffer) +
             " receive_buffer_actual_max=" + std::to_string(maximum_receive_buffer) +
             " rxq_overflow=" + (all_kernel_drop_counters_enabled ? "on" : "partial/off") +
             " event_logs=" + (config.log_host_events ? "on" : "off") +
             " allowlisted_sources=" + std::to_string(allowed_sources.size()) +
             " cpu_base=" + (config.cpu_base >= 0
                                  ? std::to_string(config.cpu_base)
                                  : std::string("disabled")));

    const int expected_reported_buffer = config.receive_buffer_bytes * 2;
    if (minimum_receive_buffer < expected_reported_buffer) {
        log_line("warning SO_RCVBUF was capped by system limits; check net.core.rmem_max before interpreting kernel_dropped");
    }


    try {
        for (const std::unique_ptr<Receiver>& receiver : receivers) {
            receiver->start();
        }
    } catch (const std::exception& exception) {
        log_line(std::string("could not start receiver threads: ") + exception.what());
        stop_receivers.store(true, std::memory_order_relaxed);
        for (const auto& receiver : receivers) {
            receiver->join();
        }
        admin_server.stop();
        return 1;
    }

    auto next_statistics = SteadyClock::now() + kStatisticsInterval;
    std::array<epoll_event, kMaximumEpollEvents> events{};
    bool stopping = false;
    bool failed = false;

    while (!stopping && !failed &&
           !receiver_failed.load(std::memory_order_relaxed)) {
        const int event_count = epoll_wait(
            epoll_fd.get(), events.data(), static_cast<int>(events.size()), 1000);
        if (event_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            log_line(std::string("epoll_wait failed: ") + std::strerror(errno));
            failed = true;
            break;
        }

        for (int event_index = 0; event_index < event_count; ++event_index) {
            if (events[event_index].data.fd != signal_fd.get()) {
                continue;
            }
            signalfd_siginfo signal_info{};
            const ssize_t bytes_read = read(signal_fd.get(), &signal_info, sizeof(signal_info));
            if (bytes_read == static_cast<ssize_t>(sizeof(signal_info))) {
                log_line("received shutdown signal=" +
                         std::to_string(signal_info.ssi_signo));
                stopping = true;
                break;
            }
        }

        const auto now = SteadyClock::now();
        if (now >= next_statistics) {
            print_statistics(counters, workers, receivers);
            next_statistics = now + kStatisticsInterval;
        }
    }

    stop_receivers.store(true, std::memory_order_relaxed);
    for (const auto& receiver : receivers) {
        receiver->join();
    }
    admin_server.stop();

    log_line("stopping: draining worker queues");
    for (const auto& worker : workers) {
        worker->stop();
    }
    print_statistics(counters, workers, receivers);
    log_line("server stopped");
    return failed || receiver_failed.load(std::memory_order_relaxed) ? 1 : 0;
}
}  // namespace heartbeat





























