#include "heartbeat/protocol.h"

#include <arpa/inet.h>
#include <algorithm>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <cerrno>
#include <iostream>
#include <netdb.h>
#include <random>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {

volatile std::sig_atomic_t g_stopping = 0;

void handle_signal(int) {
    g_stopping = 1;
}

struct ClientConfig {
    std::string server;
    std::string host_id;
    std::uint16_t port = 9000;
    std::uint32_t interval_ms = 1000;
};

void print_usage(const char* program) {
    std::cout << "Usage: " << program << " --server HOST --id HOST_ID [options]\n"
              << "  --server HOST       Server IPv4 address or DNS name\n"
              << "  --id HOST_ID        Stable host identity (1-128 ASCII safe characters)\n"
              << "  --port PORT         Server UDP port (default 9000)\n"
              << "  --interval-ms N     Heartbeat interval (default 1000)\n"
              << "  --help              Show this help\n";
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

bool read_option_value(int* index, int argc, char** argv, std::string* value) {
    if (*index + 1 >= argc) {
        return false;
    }
    ++(*index);
    *value = argv[*index];
    return true;
}

bool parse_arguments(int argc, char** argv, ClientConfig* config) {
    bool has_server = false;
    bool has_host_id = false;
    for (int index = 1; index < argc; ++index) {
        const std::string option = argv[index];
        if (option == "--help") {
            print_usage(argv[0]);
            return false;
        }

        std::string value;
        if (!read_option_value(&index, argc, argv, &value)) {
            std::cerr << "Missing value for option " << option << '\n';
            return false;
        }
        std::uint64_t number = 0;
        if (option == "--server") {
            config->server = value;
            has_server = true;
        } else if (option == "--id") {
            config->host_id = value;
            has_host_id = true;
        } else if (!parse_number(value, &number)) {
            std::cerr << "Invalid number for option " << option << '\n';
            return false;
        } else if (option == "--port" && number >= 1 && number <= 65535) {
            config->port = static_cast<std::uint16_t>(number);
        } else if (option == "--interval-ms" && number >= 100 && number <= 3600000) {
            config->interval_ms = static_cast<std::uint32_t>(number);
        } else {
            std::cerr << "Unknown option or out-of-range value: " << option << '\n';
            return false;
        }
    }

    if (!has_server || !has_host_id) {
        std::cerr << "Both --server and --id are required\n";
        return false;
    }
    if (!heartbeat::is_valid_host_id(config->host_id)) {
        std::cerr << "Invalid host ID; use 1-128 ASCII letters, digits, or . _ : -\n";
        return false;
    }
    return true;
}

std::uint64_t create_boot_id() {
    const auto clock_value = static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    try {
        std::random_device random;
        return (static_cast<std::uint64_t>(random()) << 32U) ^ random() ^ clock_value;
    } catch (...) {
        return clock_value ^ (static_cast<std::uint64_t>(getpid()) << 32U);
    }
}

bool resolve_server(const ClientConfig& config, sockaddr_in* address) {
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
        std::cerr << "No IPv4 address found for server\n";
    }
    return found;
}

}  // namespace

int main(int argc, char** argv) {
    for (int index = 1; index < argc; ++index) {
        if (std::string(argv[index]) == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }

    ClientConfig config;
    if (!parse_arguments(argc, argv, &config)) {
        if (argc == 1 || (argc > 1 && std::string(argv[1]) == "--help")) {
            print_usage(argv[0]);
        }
        return 2;
    }

    sockaddr_in server_address{};
    if (!resolve_server(config, &server_address)) {
        return 1;
    }

    const int socket_fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (socket_fd < 0) {
        std::cerr << "socket failed: " << std::strerror(errno) << '\n';
        return 1;
    }

    struct sigaction action{};
    action.sa_handler = handle_signal;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGINT, &action, nullptr) < 0 ||
        sigaction(SIGTERM, &action, nullptr) < 0) {
        std::cerr << "sigaction failed: " << std::strerror(errno) << '\n';
        close(socket_fd);
        return 1;
    }

    const std::uint64_t boot_id = create_boot_id();
    std::uint64_t sequence = 0;
    const auto interval = std::chrono::milliseconds(config.interval_ms);
    auto next_send = std::chrono::steady_clock::now();
    std::cout << "sending heartbeats host_id=" << config.host_id
              << " server=" << config.server
              << " port=" << config.port << std::endl;

    while (g_stopping == 0) {
        heartbeat::HeartbeatMessage message;
        message.host_id = config.host_id;
        message.boot_id = boot_id;
        message.sequence = ++sequence;

        std::vector<std::uint8_t> packet;
        std::string error;
        if (!heartbeat::encode_heartbeat(message, &packet, &error)) {
            std::cerr << "Could not encode heartbeat: " << error << '\n';
            close(socket_fd);
            return 1;
        }
        const ssize_t sent = sendto(
            socket_fd, packet.data(), packet.size(), 0,
            reinterpret_cast<const sockaddr*>(&server_address), sizeof(server_address));
        if (sent < 0) {
            if (errno == EINTR && g_stopping != 0) {
                break;
            }
            std::cerr << "sendto failed: " << std::strerror(errno) << '\n';
        } else if (static_cast<std::size_t>(sent) != packet.size()) {
            std::cerr << "sendto sent an incomplete datagram\n";
        }

        next_send += interval;
        const auto now = std::chrono::steady_clock::now();
        if (next_send < now) {
            next_send = now + interval;
        }
        while (g_stopping == 0 && std::chrono::steady_clock::now() < next_send) {
            const auto remaining = next_send - std::chrono::steady_clock::now();
            const auto sleep_time = std::min(remaining,
                std::chrono::steady_clock::duration(std::chrono::milliseconds(100)));
            std::this_thread::sleep_for(sleep_time);
        }
    }

    close(socket_fd);
    std::cout << "heartbeat client stopped" << std::endl;
    return 0;
}


