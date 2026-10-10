#ifndef HEARTBEAT_WORKER_H
#define HEARTBEAT_WORKER_H

#include "heartbeat/heartbeat_registry.h"
#include "heartbeat/protocol.h"
#include "latency_histogram.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace heartbeat {
namespace detail {

struct Counters {
    std::atomic<std::uint64_t> queue_dropped{0};
    std::atomic<std::uint64_t> capacity_rejected{0};
    std::atomic<std::uint64_t> stale_heartbeats{0};
    std::atomic<std::uint64_t> active_hosts{0};
    std::atomic<std::uint64_t> online_events{0};
    std::atomic<std::uint64_t> offline_events{0};
};
struct WorkerMetrics {
    std::atomic<std::size_t> queue_depth{0};
    std::atomic<std::size_t> queue_high_water{0};
    std::atomic<std::uint64_t> processed_heartbeats{0};
    std::atomic<std::uint64_t> queue_dropped{0};
    std::atomic<std::size_t> active_hosts{0};
    // Processing latency (receive time to handling) for this worker. Only the
    // worker thread writes; readers call summarize() for an approximate view.
    LatencyHistogram latency;
};

struct QueuedHeartbeat {
    HeartbeatMessage message;
    HeartbeatRegistry::TimePoint received_at;
};

class Worker {
public:
    Worker(std::size_t index,
           std::size_t queue_capacity,
           std::chrono::milliseconds timeout,
           std::size_t maximum_hosts,
           std::size_t expected_hosts,
           Counters* counters,
           bool log_host_events,
           int cpu_pin = -1);
    ~Worker();

    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    void start();
    void enqueue_batch(std::vector<QueuedHeartbeat>* batch);
    std::future<HostStatusSnapshot> request_snapshot(std::size_t limit);
    void stop();
    const WorkerMetrics& metrics() const;

private:
    struct SnapshotRequest;

    bool reserve_host_slot();
    void handle_heartbeat(const QueuedHeartbeat& queued);
    void expire_hosts(HeartbeatRegistry::TimePoint now);
    void run();

    std::size_t index_;
    std::size_t queue_capacity_;
    std::chrono::milliseconds timeout_;
    std::size_t maximum_hosts_;
    Counters* counters_;
    bool log_host_events_;
    std::chrono::milliseconds sweep_interval_;
    int cpu_pin_;
    HeartbeatRegistry registry_;
    WorkerMetrics metrics_;
    std::vector<QueuedHeartbeat> queue_;
    std::deque<std::shared_ptr<SnapshotRequest>> snapshot_requests_;
    std::size_t queue_head_ = 0;
    std::size_t queue_tail_ = 0;
    std::size_t queue_size_ = 0;
    std::mutex queue_mutex_;
    std::condition_variable queue_ready_;
    bool stopping_ = false;
    std::thread thread_;
};

}  // namespace detail
}  // namespace heartbeat

#endif  // HEARTBEAT_WORKER_H