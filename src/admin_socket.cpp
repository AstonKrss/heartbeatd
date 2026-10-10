#include "admin_socket.h"

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/time.h>
#include <unistd.h>

namespace heartbeat {

AdminSocketServer::~AdminSocketServer() {
    stop();
}

bool AdminSocketServer::start(const std::string& path,
                              CommandHandler handler,
                              std::string* error) {
    if (path.empty() || path.size() >= sizeof(((sockaddr_un*)nullptr)->sun_path)) {
        if (error != nullptr) {
            *error = "admin socket path is empty or too long";
        }
        return false;
    }

    struct stat existing{};
    if (lstat(path.c_str(), &existing) == 0) {
        if (error != nullptr) {
            *error = "admin socket path already exists; refusing to remove an unknown file";
        }
        return false;
    }
    if (errno != ENOENT) {
        if (error != nullptr) {
            *error = std::string("lstat admin socket failed: ") + std::strerror(errno);
        }
        return false;
    }

    listener_fd_ = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listener_fd_ < 0) {
        if (error != nullptr) {
            *error = std::string("admin socket failed: ") + std::strerror(errno);
        }
        return false;
    }

    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
    if (bind(listener_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        if (error != nullptr) {
            *error = std::string("bind admin socket failed: ") + std::strerror(errno);
        }
        close(listener_fd_);
        listener_fd_ = -1;
        return false;
    }
    owns_socket_path_ = true;
    path_ = path;
    struct stat created_socket{};
    if (lstat(path_.c_str(), &created_socket) < 0 || !S_ISSOCK(created_socket.st_mode)) {
        if (error != nullptr) {
            *error = "could not verify bound admin socket path";
        }
        stop();
        return false;
    }
    socket_path_device_ = created_socket.st_dev;
    socket_path_inode_ = created_socket.st_ino;

    if (chmod(path_.c_str(), S_IRUSR | S_IWUSR) < 0 || listen(listener_fd_, 16) < 0) {
        if (error != nullptr) {
            *error = std::string("configure admin socket failed: ") + std::strerror(errno);
        }
        stop();
        return false;
    }

    handler_ = std::move(handler);
    stopping_.store(false, std::memory_order_relaxed);
    try {
        thread_ = std::thread(&AdminSocketServer::run, this);
    } catch (const std::exception& exception) {
        if (error != nullptr) {
            *error = std::string("start admin socket thread failed: ") + exception.what();
        }
        stop();
        return false;
    }
    return true;
}

void AdminSocketServer::stop() {
    stopping_.store(true, std::memory_order_relaxed);
    if (thread_.joinable()) {
        thread_.join();
    }
    if (listener_fd_ >= 0) {
        close(listener_fd_);
        listener_fd_ = -1;
    }
    if (owns_socket_path_) {
        struct stat current_path{};
        if (lstat(path_.c_str(), &current_path) == 0 &&
            S_ISSOCK(current_path.st_mode) &&
            current_path.st_dev == socket_path_device_ &&
            current_path.st_ino == socket_path_inode_) {
            unlink(path_.c_str());
        }
        owns_socket_path_ = false;
    }
}

void AdminSocketServer::run() {
    while (!stopping_.load(std::memory_order_relaxed)) {
        pollfd listener{};
        listener.fd = listener_fd_;
        listener.events = POLLIN;
        const int ready = poll(&listener, 1, 250);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (ready == 0 || (listener.revents & POLLIN) == 0) {
            continue;
        }

        const int client_fd = accept4(listener_fd_, nullptr, nullptr, SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            break;
        }
        handle_client(client_fd);
        close(client_fd);
    }
}

void AdminSocketServer::handle_client(int client_fd) {
    timeval timeout{};
    timeout.tv_sec = 2;
    setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    char command_buffer[128]{};
    std::size_t used = 0;
    while (used < sizeof(command_buffer) - 1) {
        const ssize_t received = recv(client_fd, command_buffer + used,
                                      sizeof(command_buffer) - 1 - used, 0);
        if (received > 0) {
            used += static_cast<std::size_t>(received);
            if (std::memchr(command_buffer, '\n', used) != nullptr) {
                break;
            }
            continue;
        }
        if (received < 0 && errno == EINTR) {
            continue;
        }
        break;
    }

    std::string response;
    if (used == 0) {
        response = "ERR empty command\n";
    } else {
        command_buffer[used] = '\0';
        std::string command(command_buffer, used);
        while (!command.empty() && (command.back() == '\n' || command.back() == '\r')) {
            command.pop_back();
        }
        try {
            response = handler_(command);
        } catch (const std::exception& exception) {
            response = std::string("ERR ") + exception.what() + "\n";
        } catch (...) {
            response = "ERR internal admin command error\n";
        }
    }

    std::size_t sent_total = 0;
    while (sent_total < response.size()) {
        const ssize_t sent = send(client_fd, response.data() + sent_total,
                                  response.size() - sent_total, MSG_NOSIGNAL);
        if (sent > 0) {
            sent_total += static_cast<std::size_t>(sent);
            continue;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}

}  // namespace heartbeat



