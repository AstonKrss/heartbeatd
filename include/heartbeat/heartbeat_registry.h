#ifndef HEARTBEAT_REGISTRY_H
#define HEARTBEAT_REGISTRY_H

#include "heartbeat/protocol.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>

namespace heartbeat {

struct HostStatusEntry {
    std::string host_id;
    std::uint64_t last_seen_ago_ms = 0;
};

struct HostStatusSnapshot {
    std::vector<HostStatusEntry> hosts;
    bool truncated = false;
};

class HeartbeatRegistry {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    enum class UpdateResult {
        kAdded,
        kRefreshed,
        kRestarted,
        kStale,
    };

    // This registry is owned by exactly one worker thread; it needs no internal lock.
    void reserve(std::size_t expected_hosts);
    bool contains(const std::string& host_id) const;
    UpdateResult record(const HeartbeatMessage& message, TimePoint received_at);
    // Removes a host and returns whether it existed. Only needed to roll back an
    // insertion when a global host-slot check fails after record() already added it.
    bool remove(const std::string& host_id);
    std::vector<std::string> expire(TimePoint now, std::chrono::milliseconds timeout);
    HostStatusSnapshot snapshot(std::size_t limit, TimePoint now) const;
    std::size_t size() const;

private:
    struct Entry {
        std::string host_id;
        std::uint64_t boot_id = 0;
        std::uint64_t sequence = 0;
        TimePoint last_seen;
    };

    // Oldest heartbeat is at the front. Hash lookup plus list splice/removal is average O(1).
    std::list<Entry> recent_heartbeats_;
    std::unordered_map<std::string, std::list<Entry>::iterator> by_host_id_;
};

}  // namespace heartbeat

#endif  // HEARTBEAT_REGISTRY_H