#ifndef HEARTBEAT_PROTOCOL_H
#define HEARTBEAT_PROTOCOL_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace heartbeat {

constexpr std::size_t kMaximumHostIdLength = 128;
constexpr std::size_t kProtocolHeaderSize = 24;
constexpr std::size_t kMaximumDatagramSize =
    kProtocolHeaderSize + kMaximumHostIdLength;

// Byte offset of the u64 sequence field inside the fixed header. The benchmark
// reuses pre-encoded packets and only rewrites these 8 bytes before sending.
constexpr std::size_t kSequenceOffset = 16;

struct HeartbeatMessage {
    std::string host_id;
    std::uint64_t boot_id = 0;
    std::uint64_t sequence = 0;
};

// IDs are deliberately restricted to a small, log-safe ASCII alphabet.
bool is_valid_host_id(const std::string& host_id);

// Encodes/decodes the versioned wire format. Errors are returned as readable text.
bool encode_heartbeat(const HeartbeatMessage& message,
                      std::vector<std::uint8_t>* output,
                      std::string* error);
bool decode_heartbeat(const std::uint8_t* data,
                      std::size_t size,
                      HeartbeatMessage* output,
                      std::string* error);

}  // namespace heartbeat

#endif  // HEARTBEAT_PROTOCOL_H