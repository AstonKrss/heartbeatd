#include "heartbeat/heartbeat_registry.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

heartbeat::HeartbeatMessage message(
    const std::string& host_id, std::uint64_t boot_id, std::uint64_t sequence) {
    heartbeat::HeartbeatMessage result;
    result.host_id = host_id;
    result.boot_id = boot_id;
    result.sequence = sequence;
    return result;
}

}  // namespace

int main() {
    using namespace std::chrono;
    using Registry = heartbeat::HeartbeatRegistry;

    Registry registry;
    const Registry::TimePoint start{};
    const milliseconds timeout(5000);

    require(registry.record(message("node-a", 1, 1), start) ==
                Registry::UpdateResult::kAdded,
            "first heartbeat adds a host");
    require(registry.record(message("node-b", 1, 1), start + seconds(1)) ==
                Registry::UpdateResult::kAdded,
            "second host is added");
    require(registry.record(message("node-a", 1, 3), start + seconds(4)) ==
                Registry::UpdateResult::kRefreshed,
            "newer sequence refreshes a host");
    require(registry.record(message("node-a", 1, 2), start + seconds(5)) ==
                Registry::UpdateResult::kStale,
            "out-of-order sequence is ignored");

    const heartbeat::HostStatusSnapshot full_snapshot =
        registry.snapshot(2, start + seconds(5));
    require(full_snapshot.hosts.size() == 2 && !full_snapshot.truncated,
            "complete snapshot returns all online hosts");
    bool found_node_a_age = false;
    for (const heartbeat::HostStatusEntry& host : full_snapshot.hosts) {
        if (host.host_id == "node-a" && host.last_seen_ago_ms == 1000) {
            found_node_a_age = true;
        }
    }
    require(found_node_a_age, "snapshot reports monotonic age since last heartbeat");
    const heartbeat::HostStatusSnapshot limited_snapshot =
        registry.snapshot(1, start + seconds(5));
    require(limited_snapshot.hosts.size() == 1 && limited_snapshot.truncated,
            "limited snapshot signals that results were truncated");
    std::vector<std::string> expired = registry.expire(start + seconds(6), timeout);
    require(expired.size() == 1 && expired.front() == "node-b",
            "oldest expired host is removed first");
    require(registry.size() == 1, "registry count follows expiration");

    require(registry.record(message("node-a", 2, 1), start + seconds(7)) ==
                Registry::UpdateResult::kRestarted,
            "changed boot ID is recognized as a restart");
    expired = registry.expire(start + seconds(11), timeout);
    require(expired.empty(), "recent heartbeat is not expired early");
    expired = registry.expire(start + seconds(12), timeout);
    require(expired.size() == 1 && expired.front() == "node-a",
            "host expires at its configured timeout");
    require(registry.size() == 0, "registry becomes empty after expiration");

    // remove() 只服务于"新主机插入后全局名额不足"的回滚路径。
    require(registry.record(message("node-c", 1, 1), start + seconds(20)) ==
                Registry::UpdateResult::kAdded,
            "third host is added");
    require(registry.remove("node-c"), "remove deletes an existing host");
    require(!registry.remove("node-c"), "removing a missing host reports false");
    require(registry.size() == 0, "registry shrinks after removal");
    require(registry.record(message("node-c", 1, 5), start + seconds(21)) ==
                Registry::UpdateResult::kAdded,
            "a host can come back after removal");
    require(registry.expire(start + seconds(30), timeout).size() == 1,
            "re-added host still participates in expiration");
    require(registry.size() == 0, "registry is empty again");

    std::cout << "registry tests passed\n";
    return 0;
}

