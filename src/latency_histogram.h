#ifndef HEARTBEAT_LATENCY_HISTOGRAM_H
#define HEARTBEAT_LATENCY_HISTOGRAM_H

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace heartbeat {
namespace detail {

// 固定分桶的延迟直方图：记录"数据报被收到"到"被 worker 处理完"的耗时（微秒）。
// 使用约定：record() 只由单个 owner 线程调用；summarize() 可被其他线程随时调用，
// 读到的各桶计数最终会追上 owner，但一次快照允许跨桶的微小误差。
// 累计计数很小的时候百分位只是粗粒度参考，样本量足够大后才有意义。
class LatencyHistogram {
public:
    struct Summary {
        std::uint64_t sample_count = 0;
        std::uint64_t p50_us = 0;
        std::uint64_t p90_us = 0;
        std::uint64_t p99_us = 0;
        std::uint64_t p999_us = 0;
        std::uint64_t max_us = 0;
        // true 表示有样本落进溢出桶（>= 100000us），此时 max_us 只表示
        // "至少这么大"，具体数值不可知。
        bool saturated = false;
    };

    void record(std::uint64_t microseconds);

    // 汇总一次快照。百分位是桶近似值：取累计计数首次达到目标位置的桶上界，
    // 不做插值。
    Summary summarize() const;

private:
    // 有限桶按上界递增（微秒），刻度取 1/2/5 量级，覆盖常见延迟范围。
    // 最后一个桶是溢出桶，收纳所有 >= 100000us 的样本。
    static constexpr std::array<std::uint64_t, 16> kUpperBoundsUs = {
        1, 2, 5, 10, 25, 50, 100, 250, 500,
        1000, 2000, 5000, 10000, 20000, 50000, 100000};
    static constexpr std::size_t kFiniteBucketCount = kUpperBoundsUs.size();
    static constexpr std::size_t kOverflowBucket = kFiniteBucketCount;
    static constexpr std::size_t kBucketCount = kFiniteBucketCount + 1;

    static std::size_t bucket_of(std::uint64_t microseconds);
    static std::uint64_t upper_bound_of_bucket(std::size_t bucket);

    std::array<std::atomic<std::uint64_t>, kBucketCount> buckets_{};
};

inline void LatencyHistogram::record(std::uint64_t microseconds) {
    buckets_[bucket_of(microseconds)].fetch_add(1, std::memory_order_relaxed);
}

inline std::size_t LatencyHistogram::bucket_of(std::uint64_t microseconds) {
    // 桶上界已升序排列：lower_bound 找到第一个 >= 样本值的上界，即所属桶；
    // 超过全部上界时落在溢出桶。
    const auto position = std::lower_bound(
        kUpperBoundsUs.begin(), kUpperBoundsUs.end(), microseconds);
    return static_cast<std::size_t>(position - kUpperBoundsUs.begin());
}

inline std::uint64_t LatencyHistogram::upper_bound_of_bucket(std::size_t bucket) {
    if (bucket >= kFiniteBucketCount) {
        return kUpperBoundsUs.back();
    }
    return kUpperBoundsUs[bucket];
}

inline LatencyHistogram::Summary LatencyHistogram::summarize() const {
    Summary summary;
    std::array<std::uint64_t, kBucketCount> counts{};
    for (std::size_t index = 0; index < kBucketCount; ++index) {
        counts[index] = buckets_[index].load(std::memory_order_relaxed);
        summary.sample_count += counts[index];
    }
    summary.saturated = counts[kOverflowBucket] > 0;
    if (summary.sample_count == 0) {
        return summary;
    }

    // 四个分位目标位置，按千分位整数计算，避免浮点参与热路径之外也无妨。
    const std::uint64_t per_mille_targets[4] = {500, 900, 990, 999};
    std::uint64_t* percentile_outputs[4] = {
        &summary.p50_us, &summary.p90_us, &summary.p99_us, &summary.p999_us};
    for (std::size_t percentile = 0; percentile < 4; ++percentile) {
        const std::uint64_t target =
            (summary.sample_count * per_mille_targets[percentile] + 999) / 1000;
        std::uint64_t running = 0;
        std::size_t bucket = kOverflowBucket;
        for (std::size_t index = 0; index < kBucketCount; ++index) {
            running += counts[index];
            if (running >= target) {
                bucket = index;
                break;
            }
        }
        *percentile_outputs[percentile] = upper_bound_of_bucket(bucket);
    }

    for (std::size_t index = kBucketCount; index > 0; --index) {
        if (counts[index - 1] > 0) {
            summary.max_us = upper_bound_of_bucket(index - 1);
            break;
        }
    }
    return summary;
}

}  // namespace detail
}  // namespace heartbeat

#endif  // HEARTBEAT_LATENCY_HISTOGRAM_H