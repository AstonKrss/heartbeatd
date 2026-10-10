#ifndef HEARTBEAT_SERVER_H
#define HEARTBEAT_SERVER_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace heartbeat {

struct HeartbeatServerConfig {
    std::string bind_address = "127.0.0.1";
    std::string admin_socket_path;
    std::uint16_t port = 9000;
    std::size_t worker_count = 4;
    std::size_t receiver_count = 1;
    std::size_t queue_capacity_per_worker = 8192;
    std::size_t maximum_hosts = 100000;
    std::uint32_t timeout_ms = 5000;
    int receive_buffer_bytes = 4 * 1024 * 1024;
    bool log_host_events = true;
    // First CPU for optional thread pinning: receivers take cpu_base..cpu_base+R-1,
    // workers take cpu_base+R..cpu_base+R+W-1. -1 disables pinning.
    int cpu_base = -1;
    std::vector<std::string> allowed_sources;
};

// Blocks until SIGINT/SIGTERM, then drains worker queues and exits.
int run_heartbeat_server(const HeartbeatServerConfig& config);

}  // namespace heartbeat

#endif  // HEARTBEAT_SERVER_H