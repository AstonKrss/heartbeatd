#ifndef HEARTBEAT_ADMIN_SOCKET_H
#define HEARTBEAT_ADMIN_SOCKET_H

#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <sys/types.h>

namespace heartbeat {

class AdminSocketServer {
public:
    using CommandHandler = std::function<std::string(const std::string&)>;

    AdminSocketServer() = default;
    ~AdminSocketServer();

    AdminSocketServer(const AdminSocketServer&) = delete;
    AdminSocketServer& operator=(const AdminSocketServer&) = delete;

    bool start(const std::string& path,
               CommandHandler handler,
               std::string* error);
    void stop();

private:
    void run();
    void handle_client(int client_fd);

    std::string path_;
    CommandHandler handler_;
    std::atomic<bool> stopping_{false};
    int listener_fd_ = -1;
    bool owns_socket_path_ = false;
    dev_t socket_path_device_ = 0;
    ino_t socket_path_inode_ = 0;
    std::thread thread_;
};

}  // namespace heartbeat

#endif  // HEARTBEAT_ADMIN_SOCKET_H

