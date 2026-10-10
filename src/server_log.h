#ifndef HEARTBEAT_SERVER_LOG_H
#define HEARTBEAT_SERVER_LOG_H

#include <string>

namespace heartbeat {
namespace detail {

void log_line(const std::string& message);

}  // namespace detail
}  // namespace heartbeat

#endif  // HEARTBEAT_SERVER_LOG_H
