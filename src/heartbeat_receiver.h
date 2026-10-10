#ifndef HEARTBEAT_RECEIVER_H
#define HEARTBEAT_RECEIVER_H

#include "heartbeat_worker.h"

#include <arpa/inet.h>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

namespace heartbeat {
namespace detail {

struct ReceiverMetrics {
    std::atomic<std::uint64_t> datagrams_received{0};
    std::atomic<std::uint64_t> valid_heartbeats{0};
    std::atomic<std::uint64_t> malformed_datagrams{0};
    std::atomic<std::uint64_t> unauthorized_datagrams{0};
    std::atomic<std::uint64_t> kernel_dropped_datagrams{0};
    int actual_receive_buffer = 0;
    bool kernel_drop_counter_enabled = false;
};

class Receiver {
public:
    Receiver(std::size_t index,
             int socket_fd,
             int actual_receive_buffer,
             bool kernel_drop_counter_enabled,
             const std::vector<in_addr>* allowed_sources,
             std::vector<std::unique_ptr<Worker>>* workers,
             std::atomic<bool>* stopping,
             std::atomic<bool>* failed,
             int cpu_pin = -1);
    ~Receiver();

    Receiver(const Receiver&) = delete;
    Receiver& operator=(const Receiver&) = delete;

    void start();
    void join();
    const ReceiverMetrics& metrics() const;

private:
    void run_guarded();
    void run();

    std::size_t index_;
    int socket_fd_;
    const std::vector<in_addr>* allowed_sources_;
    std::vector<std::unique_ptr<Worker>>* workers_;
    std::atomic<bool>* stopping_;
    std::atomic<bool>* failed_;
    int cpu_pin_;
    ReceiverMetrics metrics_;
    std::thread thread_;
};

}  // namespace detail
}  // namespace heartbeat

#endif  // HEARTBEAT_RECEIVER_H