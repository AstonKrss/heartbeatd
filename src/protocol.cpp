#include "heartbeat/protocol.h"

#include <array>

namespace heartbeat {
namespace {

constexpr std::array<std::uint8_t, 4> kMagic = {'H', 'B', 'T', '1'};
constexpr std::uint8_t kVersion = 1;
constexpr std::uint8_t kHeartbeatType = 1;

void write_u64(std::uint64_t value, std::uint8_t* output) {
    for (int index = 7; index >= 0; --index) {
        output[index] = static_cast<std::uint8_t>(value & 0xffU);
        value >>= 8U;
    }
}

std::uint64_t read_u64(const std::uint8_t* input) {
    std::uint64_t value = 0;
    for (int index = 0; index < 8; ++index) {
        value = (value << 8U) | input[index];
    }
    return value;
}

void set_error(std::string* error, const char* message) {
    if (error != nullptr) {
        *error = message;
    }
}

}  // namespace

bool is_valid_host_id(const std::string& host_id) {
    if (host_id.empty() || host_id.size() > kMaximumHostIdLength) {
        return false;
    }

    for (unsigned char character : host_id) {
        const bool is_letter = (character >= 'a' && character <= 'z') ||
                               (character >= 'A' && character <= 'Z');
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_separator = character == '-' || character == '_' ||
                                  character == '.' || character == ':';
        if (!is_letter && !is_digit && !is_separator) {
            return false;
        }
    }
    return true;
}

bool encode_heartbeat(const HeartbeatMessage& message,
                      std::vector<std::uint8_t>* output,
                      std::string* error) {
    if (output == nullptr) {
        set_error(error, "output buffer is null");
        return false;
    }
    if (!is_valid_host_id(message.host_id)) {
        set_error(error, "host ID must be 1-128 ASCII letters, digits, or . _ : -");
        return false;
    }

    const std::size_t host_id_size = message.host_id.size();
    output->assign(kProtocolHeaderSize + host_id_size, 0);
    for (std::size_t index = 0; index < kMagic.size(); ++index) {
        (*output)[index] = kMagic[index];
    }
    (*output)[4] = kVersion;
    (*output)[5] = kHeartbeatType;
    (*output)[6] = static_cast<std::uint8_t>((host_id_size >> 8U) & 0xffU);
    (*output)[7] = static_cast<std::uint8_t>(host_id_size & 0xffU);
    write_u64(message.boot_id, output->data() + 8);
    write_u64(message.sequence, output->data() + 16);

    for (std::size_t index = 0; index < host_id_size; ++index) {
        (*output)[kProtocolHeaderSize + index] =
            static_cast<std::uint8_t>(message.host_id[index]);
    }
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool decode_heartbeat(const std::uint8_t* data,
                      std::size_t size,
                      HeartbeatMessage* output,
                      std::string* error) {
    if (data == nullptr || output == nullptr) {
        set_error(error, "input data or output message is null");
        return false;
    }
    if (size < kProtocolHeaderSize) {
        set_error(error, "datagram is shorter than the protocol header");
        return false;
    }
    for (std::size_t index = 0; index < kMagic.size(); ++index) {
        if (data[index] != kMagic[index]) {
            set_error(error, "invalid protocol magic");
            return false;
        }
    }
    if (data[4] != kVersion) {
        set_error(error, "unsupported protocol version");
        return false;
    }
    if (data[5] != kHeartbeatType) {
        set_error(error, "unsupported message type");
        return false;
    }

    const std::size_t host_id_size =
        (static_cast<std::size_t>(data[6]) << 8U) | data[7];
    if (host_id_size == 0 || host_id_size > kMaximumHostIdLength) {
        set_error(error, "invalid host ID length");
        return false;
    }
    if (size != kProtocolHeaderSize + host_id_size) {
        set_error(error, "datagram length does not match the header");
        return false;
    }

    std::string host_id;
    host_id.reserve(host_id_size);
    for (std::size_t index = 0; index < host_id_size; ++index) {
        host_id.push_back(static_cast<char>(data[kProtocolHeaderSize + index]));
    }
    if (!is_valid_host_id(host_id)) {
        set_error(error, "host ID contains invalid characters");
        return false;
    }

    output->host_id = host_id;
    output->boot_id = read_u64(data + 8);
    output->sequence = read_u64(data + 16);
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

}  // namespace heartbeat
