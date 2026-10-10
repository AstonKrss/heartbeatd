#include "heartbeat/heartbeat_registry.h"
#include <algorithm>

namespace heartbeat {

void HeartbeatRegistry::reserve(std::size_t expected_hosts) {
    by_host_id_.max_load_factor(0.75F);
    by_host_id_.reserve(expected_hosts);
}

bool HeartbeatRegistry::contains(const std::string& host_id) const {
    return by_host_id_.find(host_id) != by_host_id_.end();
}

HeartbeatRegistry::UpdateResult HeartbeatRegistry::record(
    const HeartbeatMessage& message, TimePoint received_at) {
    const auto existing = by_host_id_.find(message.host_id);
    if (existing == by_host_id_.end()) {
        recent_heartbeats_.push_back(
            Entry{message.host_id, message.boot_id, message.sequence, received_at});
        auto new_entry = recent_heartbeats_.end();
        --new_entry;
        by_host_id_.emplace(new_entry->host_id, new_entry);
        return UpdateResult::kAdded;
    }

    auto entry = existing->second;
    if (entry->boot_id == message.boot_id && message.sequence <= entry->sequence) {
        return UpdateResult::kStale;
    }

    const bool restarted = entry->boot_id != message.boot_id;
    entry->boot_id = message.boot_id;
    entry->sequence = message.sequence;
    entry->last_seen = received_at;
    recent_heartbeats_.splice(recent_heartbeats_.end(), recent_heartbeats_, entry);

    if (restarted) {
        return UpdateResult::kRestarted;
    }
    return UpdateResult::kRefreshed;
}

bool HeartbeatRegistry::remove(const std::string& host_id) {
    const auto existing = by_host_id_.find(host_id);
    if (existing == by_host_id_.end()) {
        return false;
    }
    recent_heartbeats_.erase(existing->second);
    by_host_id_.erase(existing);
    return true;
}

std::vector<std::string> HeartbeatRegistry::expire(
    TimePoint now, std::chrono::milliseconds timeout) {
    std::vector<std::string> expired_hosts;
    while (!recent_heartbeats_.empty()) {
        const Entry& oldest = recent_heartbeats_.front();
        if (now < oldest.last_seen || now - oldest.last_seen < timeout) {
            break;
        }
        expired_hosts.push_back(oldest.host_id);
        by_host_id_.erase(oldest.host_id);
        recent_heartbeats_.pop_front();
    }
    return expired_hosts;
}

HostStatusSnapshot HeartbeatRegistry::snapshot(std::size_t limit, TimePoint now) const {
    HostStatusSnapshot snapshot;
    snapshot.hosts.reserve(std::min(limit, by_host_id_.size()));
    for (const auto& host : by_host_id_) {
        if (snapshot.hosts.size() >= limit) {
            break;
        }
        const auto entry = host.second;
        const auto age = now >= entry->last_seen
            ? std::chrono::duration_cast<std::chrono::milliseconds>(now - entry->last_seen).count()
            : 0;
        snapshot.hosts.push_back(HostStatusEntry{
            entry->host_id, static_cast<std::uint64_t>(age)});
    }
    snapshot.truncated = by_host_id_.size() > snapshot.hosts.size();
    return snapshot;
}
std::size_t HeartbeatRegistry::size() const {
    return by_host_id_.size();
}

}  // namespace heartbeat