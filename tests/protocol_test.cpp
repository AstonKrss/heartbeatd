#include "heartbeat/protocol.h"

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

}  // namespace

int main() {
    heartbeat::HeartbeatMessage original;
    original.host_id = "node-01.example:443";
    original.boot_id = 0x0123456789abcdefULL;
    original.sequence = 9876543210123ULL;

    std::vector<std::uint8_t> packet;
    std::string error;
    require(heartbeat::encode_heartbeat(original, &packet, &error),
            "valid heartbeat encodes");
    require(packet.size() == heartbeat::kProtocolHeaderSize + original.host_id.size(),
            "packet has expected size");

    heartbeat::HeartbeatMessage decoded;
    require(heartbeat::decode_heartbeat(packet.data(), packet.size(), &decoded, &error),
            "valid heartbeat decodes");
    require(decoded.host_id == original.host_id, "host ID round trips");
    require(decoded.boot_id == original.boot_id, "boot ID round trips");
    require(decoded.sequence == original.sequence, "sequence round trips");

    require(!heartbeat::decode_heartbeat(packet.data(), 3, &decoded, &error),
            "short packet is rejected");

    std::vector<std::uint8_t> trailing_bytes = packet;
    trailing_bytes.push_back(0);
    require(!heartbeat::decode_heartbeat(
                trailing_bytes.data(), trailing_bytes.size(), &decoded, &error),
            "unexpected trailing bytes are rejected");

    std::vector<std::uint8_t> bad_version = packet;
    bad_version[4] = 99;
    require(!heartbeat::decode_heartbeat(
                bad_version.data(), bad_version.size(), &decoded, &error),
            "unsupported version is rejected");

    heartbeat::HeartbeatMessage invalid = original;
    invalid.host_id = "node one";
    require(!heartbeat::encode_heartbeat(invalid, &packet, &error),
            "host ID with spaces is rejected");

    invalid.host_id.assign(heartbeat::kMaximumHostIdLength + 1, 'a');
    require(!heartbeat::encode_heartbeat(invalid, &packet, &error),
            "overlong host ID is rejected");

    std::cout << "protocol tests passed\n";
    return 0;
}
