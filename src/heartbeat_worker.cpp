#include "heartbeat_worker.h"

#include "server_log.h"
#include "thread_affinity.h"

#include <algorithm>
#include <utility>

namespace heartbeat {
namespace detail {
namespace {

using SteadyClock = HeartbeatRegistry::Clock;
constexpr std::size_t kMaximumWorkerBatchSize = 256;

}  // namespace

struct Worker::SnapshotRequest {
    explicit SnapshotRequest(std::size_t requested_limit) : limit(requested_limit) {}
    std::size_t limit;
    std::promise<HostStatusSnapshot> result;
};

Worker::Worker(std::size_t index,
               std::size_t queue_capacity,
               std::chrono::milliseconds timeout,
               std::size_t maximum_hosts,
               std::size_t expected_hosts,
               Counters* counters,
               bool log_host_events,
               int cpu_pin)
    : index_(index),
      queue_capacity_(queue_capacity),
      timeout_(timeout),
      maximum_hosts_(maximum_hosts),
      counters_(counters),
      log_host_events_(log_host_events),
      sweep_interval_(std::min(timeout / 4, std::chrono::milliseconds(1000))),
      cpu_pin_(cpu_pin),
      queue_(queue_capacity) {
    registry_.reserve(expected_hosts);
}

Worker::~Worker() {
    stop();
}

void Worker::start() {
    thread_ = std::thread(&Worker::run, this);
}

void Worker::enqueue_batch(std::vector<QueuedHeartbeat>* batch) {
    if (batch == nullptr || batch->empty()) {
        return;
    }

    std::lock_guard<std::mutex> lock(queue_mutex_);
    const std::size_t available = queue_capacity_ - queue_size_;
    const std::size_t accepted = std::min(available, batch->size());
    for (std::size_t index = 0; index < accepted; ++index) {
        queue_[queue_tail_] = std::move((*batch)[index]);
        queue_tail_ = (queue_tail_ + 1) % queue_capacity_;
        ++queue_size_;
    }

    const std::size_t dropped = batch->size() - accepted;
    if (dropped > 0) {
        counters_->queue_dropped.fetch_add(dropped, std::memory_order_relaxed);
        metrics_.queue_dropped.fetch_add(dropped, std::memory_order_relaxed);
    }

    metrics_.queue_depth.store(queue_size_, std::memory_order_relaxed);
    const std::size_t old_high_water =
        metrics_.queue_high_water.load(std::memory_order_relaxed);
    if (queue_size_ > old_high_water) {
        metrics_.queue_high_water.store(queue_size_, std::memory_order_relaxed);
    }
    if (accepted > 0) {
        queue_ready_.notify_one();
    }
}

std::future<HostStatusSnapshot> Worker::request_snapshot(std::size_t limit) {
    auto request = std::make_shared<SnapshotRequest>(limit);
    std::future<HostStatusSnapshot> result = request->result.get_future();
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (stopping_) {
            request->result.set_value(HostStatusSnapshot{});
            return result;
        }
        snapshot_requests_.push_back(request);
    }
    queue_ready_.notify_one();
    return result;
}

void Worker::stop() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stopping_ = true;
    }
    queue_ready_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
}

const WorkerMetrics& Worker::metrics() const {
    return metrics_;
}

bool Worker::reserve_host_slot() {
    std::uint64_t current = counters_->active_hosts.load(std::memory_order_relaxed);
    while (current < maximum_hosts_) {
        if (counters_->active_hosts.compare_exchange_weak(
                current, current + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void Worker::handle_heartbeat(const QueuedHeartbeat& queued) {
    // 只做一次哈希查找：新主机先插入注册表，再做全局名额校验，
    // 拿不到名额时回滚，避免"contains + record"的重复查找。
    const HeartbeatRegistry::UpdateResult result =
        registry_.record(queued.message, queued.received_at);
    switch (result) {
        case HeartbeatRegistry::UpdateResult::kAdded:
            if (!reserve_host_slot()) {
                registry_.remove(queued.message.host_id);
                counters_->capacity_rejected.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            counters_->online_events.fetch_add(1, std::memory_order_relaxed);
            if (log_host_events_) {
                log_line("online host=" + queued.message.host_id +
                         " worker=" + std::to_string(index_));
            }
            break;
        case HeartbeatRegistry::UpdateResult::kRestarted:
            if (log_host_events_) {
                log_line("restarted host=" + queued.message.host_id +
                         " worker=" + std::to_string(index_));
            }
            break;
        case HeartbeatRegistry::UpdateResult::kStale:
            counters_->stale_heartbeats.fetch_add(1, std::memory_order_relaxed);
            break;
        case HeartbeatRegistry::UpdateResult::kRefreshed:
            break;
    }
}

void Worker::expire_hosts(HeartbeatRegistry::TimePoint now) {
    const std::vector<std::string> expired = registry_.expire(now, timeout_);
    for (const std::string& host_id : expired) {
        counters_->active_hosts.fetch_sub(1, std::memory_order_relaxed);
        counters_->offline_events.fetch_add(1, std::memory_order_relaxed);
        if (log_host_events_) {
            log_line("offline host=" + host_id +
                     " timeout_ms=" + std::to_string(timeout_.count()) +
                     " worker=" + std::to_string(index_));
        }
    }
    metrics_.active_hosts.store(registry_.size(), std::memory_order_relaxed);
}

void Worker::run() {
    std::vector<QueuedHeartbeat> batch;
    batch.reserve(kMaximumWorkerBatchSize);

    if (cpu_pin_ >= 0) {
        std::string pin_error;
        if (!pin_current_thread_to_cpu(cpu_pin_, &pin_error)) {
            log_line("warning worker=" + std::to_string(index_) +
                     " could not pin to cpu=" + std::to_string(cpu_pin_) +
                     " error=" + pin_error);
        }
    }

    while (true) {
        batch.clear();
        std::shared_ptr<SnapshotRequest> snapshot_request;
        bool should_exit = false;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_ready_.wait_for(lock, sweep_interval_, [this] {
                return stopping_ || queue_size_ > 0 || !snapshot_requests_.empty();
            });

            if (!snapshot_requests_.empty()) {
                snapshot_request = snapshot_requests_.front();
                snapshot_requests_.pop_front();
            } else if (stopping_ && queue_size_ == 0) {
                should_exit = true;
            } else {
                while (queue_size_ > 0 && batch.size() < kMaximumWorkerBatchSize) {
                    batch.push_back(std::move(queue_[queue_head_]));
                    queue_head_ = (queue_head_ + 1) % queue_capacity_;
                    --queue_size_;
                }
                metrics_.queue_depth.store(queue_size_, std::memory_order_relaxed);
            }
        }

        if (should_exit) {
            break;
        }
        if (snapshot_request) {
            snapshot_request->result.set_value(
                registry_.snapshot(snapshot_request->limit, SteadyClock::now()));
            continue;
        }

        // 每个批次取一次时钟：既用于处理延迟统计，也用于本批之后的超时扫描。
        const auto batch_now = SteadyClock::now();
        for (const QueuedHeartbeat& heartbeat : batch) {
            handle_heartbeat(heartbeat);
            // 延迟从"在 reactor 收到"计到"本批开始处理"：同批后面的包会包含
            // 同批前面包的处理时间，是近似值；批次越大近似越粗。
            const auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                batch_now - heartbeat.received_at).count();
            metrics_.latency.record(static_cast<std::uint64_t>(latency_us));
            metrics_.processed_heartbeats.fetch_add(1, std::memory_order_relaxed);
        }
        expire_hosts(batch_now);
    }
    expire_hosts(SteadyClock::now());
}

}  // namespace detail
}  // namespace heartbeat