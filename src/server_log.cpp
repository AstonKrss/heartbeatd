#include "server_log.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>

namespace heartbeat {
namespace detail {
namespace {

std::mutex g_log_mutex;

}  // namespace

void log_line(const std::string& message) {
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm local_time{};
    localtime_r(&now_time, &local_time);

    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::cout << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S")
              << " " << message << std::endl;
}

}  // namespace detail
}  // namespace heartbeat

