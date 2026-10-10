#include <charconv>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace {

void print_usage(const char* program) {
    std::cout << "Usage: " << program << " --socket PATH [--stats | --list [LIMIT]]\n"
              << "  --socket PATH   Server's local Unix-domain admin socket\n"
              << "  --stats         Show counters and per-worker queues (default)\n"
              << "  --receivers     Show per-receiver counters only\n"
              << "  --list [LIMIT]  List up to LIMIT online hosts (default 1000)\n";
}

bool parse_limit(const std::string& text, std::size_t* limit) {
    std::size_t value = 0;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() ||
        value == 0 || value > 10000) {
        return false;
    }
    *limit = value;
    return true;
}

bool send_all(int socket_fd, const std::string& message) {
    std::size_t sent_total = 0;
    while (sent_total < message.size()) {
        const ssize_t sent = send(socket_fd, message.data() + sent_total,
                                  message.size() - sent_total, MSG_NOSIGNAL);
        if (sent > 0) {
            sent_total += static_cast<std::size_t>(sent);
        } else if (sent < 0 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    std::string socket_path;
    std::string command = "STATS";
    bool has_command = false;

    for (int index = 1; index < argc; ++index) {
        const std::string option = argv[index];
        if (option == "--help") {
            print_usage(argv[0]);
            return 0;
        }
        if (option == "--socket" && index + 1 < argc) {
            socket_path = argv[++index];
        } else if (option == "--stats") {
            command = "STATS";
            has_command = true;
        } else if (option == "--receivers") {
            command = "RECEIVER";
            has_command = true;
        } else if (option == "--list") {
            std::size_t limit = 1000;
            if (index + 1 < argc && argv[index + 1][0] != '-') {
                if (!parse_limit(argv[++index], &limit)) {
                    std::cerr << "LIST limit must be between 1 and 10000\n";
                    return 2;
                }
            }
            command = "LIST " + std::to_string(limit);
            has_command = true;
        } else {
            std::cerr << "Unknown option or missing value: " << option << '\n';
            print_usage(argv[0]);
            return 2;
        }
    }

    if (socket_path.empty()) {
        std::cerr << "--socket is required\n";
        print_usage(argv[0]);
        return 2;
    }
    if (socket_path.size() >= sizeof(((sockaddr_un*)nullptr)->sun_path)) {
        std::cerr << "Unix-domain socket path is too long\n";
        return 2;
    }
    if (!has_command) {
        command = "STATS";
    }

    const int socket_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (socket_fd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, socket_path.c_str(), socket_path.size() + 1);
    if (connect(socket_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        perror("connect");
        close(socket_fd);
        return 1;
    }

    if (!send_all(socket_fd, command + "\n")) {
        perror("send");
        close(socket_fd);
        return 1;
    }
    shutdown(socket_fd, SHUT_WR);

    char buffer[4096];
    while (true) {
        const ssize_t received = read(socket_fd, buffer, sizeof(buffer));
        if (received > 0) {
            std::cout.write(buffer, received);
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received < 0) {
            perror("read");
            close(socket_fd);
            return 1;
        }
        break;
    }

    close(socket_fd);
    return 0;
}

