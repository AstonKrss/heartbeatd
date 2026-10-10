#ifndef HEARTBEAT_THREAD_AFFINITY_H
#define HEARTBEAT_THREAD_AFFINITY_H

#include <pthread.h>
#include <sched.h>
#include <cstring>
#include <string>

namespace heartbeat {
namespace detail {

// 线程亲和性属于可选优化：把接收 reactor 和 worker 固定到指定 CPU，
// 可以减少跨核迁移和缓存失效，适合绑定专用核的生产部署。
// 它依赖进程的 cpuset（容器里同样按可见 CPU 编号解释），失败不应该致命，
// 调用方记录告警后继续运行即可。

// 把当前线程绑定到指定 CPU 编号。失败时把可读原因写进 error 并返回 false。
inline bool pin_current_thread_to_cpu(int cpu, std::string* error) {
    if (cpu < 0 || cpu >= CPU_SETSIZE) {
        if (error != nullptr) {
            *error = "cpu index out of range";
        }
        return false;
    }
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(cpu, &cpu_set);
    const int result =
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
    if (result != 0) {
        if (error != nullptr) {
            *error = std::strerror(result);
        }
        return false;
    }
    return true;
}

// 当前进程允许运行的 CPU 数量；读取失败时返回 0。
inline int available_cpu_count() {
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    if (sched_getaffinity(0, sizeof(cpu_set), &cpu_set) != 0) {
        return 0;
    }
    return CPU_COUNT(&cpu_set);
}

}  // namespace detail
}  // namespace heartbeat

#endif  // HEARTBEAT_THREAD_AFFINITY_H