#include "heartbeat/protocol.h"

#include <arpa/inet.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

constexpr std::size_t kBatchSize = 32;

// 一台模拟主机：host_id 固定，预编码一份数据报，发送前只改写 sequence。
struct HostTemplate {
    std::string host_id;
    std::uint64_t sequence = 0;
    std::vector<std::uint8_t> packet;
};

// 一个源流：一个 socket 加一组主机。同一个流的数据报有相同的源端口，
// 服务端 SO_REUSEPORT 按流哈希选 socket，这样压测能覆盖多个接收 reactor。
struct FlowSender {
    int socket_fd = -1;
    std::vector<HostTemplate> hosts;
    std::size_t next_host = 0;
    std::array<iovec, kBatchSize> data_vectors{};
    std::array<mmsghdr, kBatchSize> messages{};
};

struct BenchConfig {
    std::string server;
    std::uint16_t port = 9000;
    std::size_t host_count = 10000;
    std::size_t thread_count = 1;
    std::size_t flow_count = 0;
    std::uint64_t packets_per_second = 10000;
    std::uint32_t duration_seconds = 10;
};

void print_usage(const char* program) {
    std::cout << "Usage: " << program << " --server HOST [options]\n"
              << "  --server HOST         Server IPv4 address or DNS name\n"
              << "  --port PORT           Server UDP port (default 9000)\n"
              << "  --hosts COUNT         Simulated host IDs (default 10000)\n"
              << "  --threads COUNT       Sender threads (default 1)\n"
              << "  --flows COUNT         UDP source sockets/flows (default: same as threads)\n"
              << "  --rate-pps COUNT      Target total packets/second (default 10000)\n"
              << "  --duration-sec COUNT  Test duration (default 10)\n";
}

bool parse_number(const std::string& text, std::uint64_t* value) {
    if (text.empty() || value == nullptr) {
        return false;
    }
    std::uint64_t parsed = 0;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
        return false;
    }
    *value = parsed;
    return true;
}

bool parse_arguments(int argc, char** argv, BenchConfig* config) {
    bool has_server = false;
    for (int index = 1; index < argc; ++index) {
        const std::string option = argv[index];
        if (option == "--help") {
            print_usage(argv[0]);
            return false;
        }
        if (index + 1 >= argc) {
            std::cerr << "Missing value for option " << option << '\n';
            return false;
        }

        const std::string value = argv[++index];
        std::uint64_t number = 0;
        if (option == "--server") {
            config->server = value;
            has_server = true;
        } else if (!parse_number(value, &number)) {
            std::cerr << "Invalid number for option " << option << '\n';
            return false;
        } else if (option == "--port" && number >= 1 && number <= 65535) {
            config->port = static_cast<std::uint16_t>(number);
        } else if (option == "--hosts" && number >= 1 && number <= 1000000) {
            config->host_count = static_cast<std::size_t>(number);
        } else if (option == "--threads" && number >= 1 && number <= 256) {
            config->thread_count = static_cast<std::size_t>(number);
        } else if (option == "--flows" && number >= 1 && number <= 1024) {
            config->flow_count = static_cast<std::size_t>(number);
        } else if (option == "--rate-pps" && number >= 1 && number <= 100000000) {
            config->packets_per_second = number;
        } else if (option == "--duration-sec" && number >= 1 && number <= 3600) {
            config->duration_seconds = static_cast<std::uint32_t>(number);
        } else {
            std::cerr << "Unknown option or out-of-range value: " << option << '\n';
            return false;
        }
    }

    if (config->flow_count == 0) {
        config->flow_count = config->thread_count;
    }
    if (!has_server || config->host_count < config->thread_count ||
        config->flow_count < config->thread_count ||
        config->flow_count > config->host_count ||
        config->packets_per_second < config->thread_count) {
        std::cerr << "Specify --server; hosts, flows, and rate must be compatible with thread count\n";
        return false;
    }
    return true;
}

bool resolve_server(const BenchConfig& config, sockaddr_in* address) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* results = nullptr;
    const std::string port = std::to_string(config.port);
    const int result = getaddrinfo(config.server.c_str(), port.c_str(), &hints, &results);
    if (result != 0) {
        std::cerr << "Could not resolve server: " << gai_strerror(result) << '\n';
        return false;
    }

    bool found = false;
    for (addrinfo* current = results; current != nullptr; current = current->ai_next) {
        if (current->ai_family == AF_INET && current->ai_addrlen >= sizeof(sockaddr_in)) {
            std::memcpy(address, current->ai_addr, sizeof(sockaddr_in));
            found = true;
            break;
        }
    }
    freeaddrinfo(results);
    if (!found) {
        std::cerr << "Server did not resolve to an IPv4 address\n";
    }
    return found;
}

void close_flow_sockets(std::vector<FlowSender>* flows) {
    for (FlowSender& flow : *flows) {
        if (flow.socket_fd >= 0) {
            close(flow.socket_fd);
            flow.socket_fd = -1;
        }
    }
}

// 协议头里的 sequence 是大端 u64，偏移固定。预编码之后每包只重写这 8 个字节，
// 发送线程不再重复做字符串拼装和合法性检查。
void patch_sequence(std::uint8_t* packet, std::uint64_t sequence) {
    for (int index = 7; index >= 0; --index) {
        packet[heartbeat::kSequenceOffset + index] =
            static_cast<std::uint8_t>(sequence & 0xffU);
        sequence >>= 8U;
    }
}

void send_from_thread(const BenchConfig& config,
                      const sockaddr_in& server_address,
                      std::size_t thread_index,
                      std::size_t first_host,
                      std::size_t end_host,
                      std::size_t flow_count,
                      std::uint64_t thread_rate,
                      std::atomic<std::uint64_t>* sent_packets,
                      std::atomic<std::uint64_t>* failed_packets) {
    std::vector<FlowSender> flows(flow_count);
    for (FlowSender& flow : flows) {
        flow.socket_fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
        if (flow.socket_fd < 0) {
            std::cerr << "socket failed: " << std::strerror(errno) << '\n';
            failed_packets->fetch_add(1, std::memory_order_relaxed);
            close_flow_sockets(&flows);
            return;
        }
    }

    const std::uint64_t clock_seed = static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const std::uint64_t boot_id = clock_seed ^
        (static_cast<std::uint64_t>(getpid()) << 32U) ^ thread_index;

    // 每个 host 只编码一次：boot_id 和 host_id 不变，之后只更新 sequence。
    for (std::size_t host_index = first_host; host_index < end_host; ++host_index) {
        const std::size_t flow_index = (host_index - first_host) % flow_count;
        FlowSender& flow = flows[flow_index];

        heartbeat::HeartbeatMessage template_message;
        template_message.host_id = "bench-host-" + std::to_string(host_index);
        template_message.boot_id = boot_id;
        template_message.sequence = 0;

        HostTemplate host;
        host.host_id = template_message.host_id;
        std::string encode_error;
        if (!heartbeat::encode_heartbeat(template_message, &host.packet, &encode_error)) {
            std::cerr << "encode failed for " << host.host_id << ": "
                      << encode_error << '\n';
            failed_packets->fetch_add(end_host - host_index, std::memory_order_relaxed);
            close_flow_sockets(&flows);
            return;
        }
        flow.hosts.push_back(std::move(host));
    }

    // mmsghdr 的固定部分只设置一次；每轮循环只改 iov 指向的缓冲区和序列号。
    for (FlowSender& flow : flows) {
        for (std::size_t index = 0; index < kBatchSize; ++index) {
            flow.messages[index] = {};
            flow.messages[index].msg_hdr.msg_name =
                const_cast<sockaddr_in*>(&server_address);
            flow.messages[index].msg_hdr.msg_namelen = sizeof(server_address);
            flow.messages[index].msg_hdr.msg_iov = &flow.data_vectors[index];
            flow.messages[index].msg_hdr.msg_iovlen = 1;
        }
    }

    const std::size_t batch_size = thread_rate >= kBatchSize ? kBatchSize : 1;
    const auto batch_interval = std::chrono::nanoseconds(
        static_cast<std::int64_t>(1000000000ULL * batch_size / thread_rate));
    const auto stop_time = std::chrono::steady_clock::now() +
                           std::chrono::seconds(config.duration_seconds);
    auto next_batch = std::chrono::steady_clock::now();
    std::size_t next_flow = 0;

    while (std::chrono::steady_clock::now() < stop_time) {
        FlowSender& flow = flows[next_flow];
        if (flow.hosts.empty()) {
            // 防御：线程分到的流数多于主机数时会有空流，直接跳过。
            next_flow = (next_flow + 1) % flow_count;
            continue;
        }
        const std::size_t current_batch_size = batch_size;

        for (std::size_t index = 0; index < current_batch_size; ++index) {
            HostTemplate& host = flow.hosts[flow.next_host];
            flow.next_host = (flow.next_host + 1) % flow.hosts.size();

            ++host.sequence;
            patch_sequence(host.packet.data(), host.sequence);

            flow.data_vectors[index].iov_base = host.packet.data();
            flow.data_vectors[index].iov_len = host.packet.size();
        }

        const int sent = sendmmsg(
            flow.socket_fd, flow.messages.data(),
            static_cast<unsigned int>(current_batch_size), 0);
        if (sent < 0) {
            failed_packets->fetch_add(current_batch_size, std::memory_order_relaxed);
        } else {
            sent_packets->fetch_add(static_cast<std::uint64_t>(sent),
                                    std::memory_order_relaxed);
            failed_packets->fetch_add(current_batch_size - static_cast<std::size_t>(sent),
                                      std::memory_order_relaxed);
        }

        next_flow = (next_flow + 1) % flow_count;
        next_batch += batch_interval;
        const auto now = std::chrono::steady_clock::now();
        if (next_batch > now) {
            std::this_thread::sleep_until(next_batch);
        } else {
            next_batch = now;
        }
    }

    close_flow_sockets(&flows);
}

}  // namespace

int main(int argc, char** argv) {
    for (int index = 1; index < argc; ++index) {
        if (std::string(argv[index]) == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }

    BenchConfig config;
    if (!parse_arguments(argc, argv, &config)) {
        if (argc == 1) {
            print_usage(argv[0]);
        }
        return 2;
    }

    sockaddr_in server_address{};
    if (!resolve_server(config, &server_address)) {
        return 1;
    }

    std::atomic<std::uint64_t> sent_packets{0};
    std::atomic<std::uint64_t> failed_packets{0};
    std::vector<std::thread> threads;
    threads.reserve(config.thread_count);

    const std::size_t hosts_per_thread = config.host_count / config.thread_count;
    const std::size_t remaining_hosts = config.host_count % config.thread_count;
    const std::size_t flows_per_thread = config.flow_count / config.thread_count;
    const std::size_t remaining_flows = config.flow_count % config.thread_count;
    const std::uint64_t rate_per_thread = config.packets_per_second / config.thread_count;
    const std::uint64_t extra_rate = config.packets_per_second % config.thread_count;
    std::size_t first_host = 0;
    const auto start_time = std::chrono::steady_clock::now();

    for (std::size_t index = 0; index < config.thread_count; ++index) {
        const std::size_t host_count = hosts_per_thread + (index < remaining_hosts ? 1 : 0);
        const std::size_t flow_count = flows_per_thread + (index < remaining_flows ? 1 : 0);
        const std::uint64_t rate = rate_per_thread + (index < extra_rate ? 1 : 0);
        threads.emplace_back(send_from_thread, std::cref(config),
                             std::cref(server_address), index,
                             first_host, first_host + host_count, flow_count, rate,
                             &sent_packets, &failed_packets);
        first_host += host_count;
    }

    for (std::thread& thread : threads) {
        thread.join();
    }

    const double elapsed_seconds = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - start_time).count();
    const std::uint64_t sent = sent_packets.load(std::memory_order_relaxed);
    std::cout << "benchmark complete hosts=" << config.host_count
              << " threads=" << config.thread_count
              << " flows=" << config.flow_count
              << " target_pps=" << config.packets_per_second
              << " sent=" << sent
              << " failed=" << failed_packets.load(std::memory_order_relaxed)
              << " elapsed_seconds=" << elapsed_seconds
              << " actual_pps=" << (elapsed_seconds > 0 ? sent / elapsed_seconds : 0)
              << '\n';
    return failed_packets.load(std::memory_order_relaxed) == 0 ? 0 : 1;
}