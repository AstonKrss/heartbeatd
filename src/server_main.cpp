#include "heartbeat/heartbeat_server.h"

#include <arpa/inet.h>
#include <charconv>
#include <cstdint>
#include <iostream>
#include <string>

namespace {

void print_usage(const char* program) {
    std::cout << "Usage: " << program << " [options]\n"
              << "  --bind IPv4                 Bind address (default 127.0.0.1)\n"
              << "  --port PORT                 UDP port (default 9000)\n"
              << "  --workers COUNT             Worker threads (default 4)\n"
              << "  --receivers COUNT           SO_REUSEPORT receive reactors (default 1)\n"
              << "  --cpu-base N                Pin receivers then workers to CPUs starting at N (default: disabled)\n"
              << "  --queue-capacity N          Queue slots per worker (default 8192)\n"
              << "  --max-hosts N               Maximum online hosts (default 100000)\n"
              << "  --timeout-ms N              Offline timeout (default 5000)\n"
              << "  --receive-buffer-bytes N    Requested SO_RCVBUF (default 4194304)\n"
              << "  --allow-source IPv4         Allow only this source; may be repeated\n"
              << "  --admin-socket PATH         Enable local LIST/STATS socket\n"
              << "  --quiet-events              Disable per-host transition logs\n"
              << "  --help                      Show this help\n";
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

bool is_ipv4_address(const std::string& value) {
    in_addr address{};
    return inet_pton(AF_INET, value.c_str(), &address) == 1;
}

}  // namespace

int main(int argc, char** argv) {
    heartbeat::HeartbeatServerConfig config;
    for (int index = 1; index < argc; ++index) {
        const std::string option = argv[index];
        if (option == "--quiet-events") {
            config.log_host_events = false;
            continue;
        }
        if (option == "--help") {
            print_usage(argv[0]);
            return 0;
        }

        std::string value;
        if (!read_option_value(&index, argc, argv, &value)) {
            std::cerr << "Missing value for option " << option << '\n';
            print_usage(argv[0]);
            return 2;
        }

        std::uint64_t number = 0;
        if (option == "--bind") {
            config.bind_address = value;
        } else if (option == "--admin-socket") {
            config.admin_socket_path = value;
        } else if (option == "--allow-source") {
            if (!is_ipv4_address(value)) {
                std::cerr << "--allow-source requires an IPv4 address\n";
                return 2;
            }
            config.allowed_sources.push_back(value);
        } else if (!parse_number(value, &number)) {
            std::cerr << "Invalid number for option " << option << '\n';
            return 2;
        } else if (option == "--port" && number >= 1 && number <= 65535) {
            config.port = static_cast<std::uint16_t>(number);
        } else if (option == "--workers" && number >= 1 && number <= 256) {
            config.worker_count = static_cast<std::size_t>(number);
        } else if (option == "--receivers" && number >= 1 && number <= 64) {
            config.receiver_count = static_cast<std::size_t>(number);
        } else if (option == "--cpu-base" && number <= 4096) {
            config.cpu_base = static_cast<int>(number);
        } else if (option == "--queue-capacity" && number >= 1 && number <= 1000000) {
            config.queue_capacity_per_worker = static_cast<std::size_t>(number);
        } else if (option == "--max-hosts" && number >= 1 && number <= 1000000) {
            config.maximum_hosts = static_cast<std::size_t>(number);
        } else if (option == "--timeout-ms" && number >= 100 && number <= 86400000) {
            config.timeout_ms = static_cast<std::uint32_t>(number);
        } else if (option == "--receive-buffer-bytes" &&
                   number >= 65536 && number <= 67108864) {
            config.receive_buffer_bytes = static_cast<int>(number);
        } else {
            std::cerr << "Unknown option or out-of-range value: " << option << '\n';
            print_usage(argv[0]);
            return 2;
        }
    }
    return heartbeat::run_heartbeat_server(config);
}





