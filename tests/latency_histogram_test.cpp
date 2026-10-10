#include "latency_histogram.h"

#include <cstdlib>
#include <iostream>
#include <string>

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

}  // namespace

int main() {
    using heartbeat::detail::LatencyHistogram;

    LatencyHistogram histogram;
    require(histogram.summarize().sample_count == 0,
            "empty histogram has no samples");
    require(histogram.summarize().p50_us == 0,
            "empty histogram reports zero percentiles");

    // 单样本：所有分位都落到同一个桶。
    histogram.record(10);
    LatencyHistogram::Summary single = histogram.summarize();
    require(single.sample_count == 1, "single sample is counted");
    require(single.p50_us == 10 && single.p999_us == 10 && single.max_us == 10,
            "single sample pins every percentile to its bucket");
    require(!single.saturated, "single sample below the overflow bound");

    // 均匀样本：p50 应落在中位桶，p99 覆盖长尾。
    LatencyHistogram mixed;
    for (int index = 0; index < 900; ++index) {
        mixed.record(100);
    }
    for (int index = 0; index < 100; ++index) {
        mixed.record(5000);
    }
    const LatencyHistogram::Summary summary = mixed.summarize();
    require(summary.sample_count == 1000, "all samples are counted");
    require(summary.p50_us == 100, "p50 stays in the fast bucket");
    require(summary.p99_us == 5000, "p99 catches the slow tail");
    require(summary.max_us == 5000, "max reflects the slowest bucket");
    require(!summary.saturated, "no overflow samples recorded");

    // 超过最高有限桶上界的样本进溢出桶。
    LatencyHistogram saturated;
    saturated.record(1000000);
    const LatencyHistogram::Summary slow = saturated.summarize();
    require(slow.sample_count == 1, "overflow sample is counted");
    require(slow.saturated, "overflow bucket flags the summary as saturated");
    require(slow.max_us == 100000, "saturated max reports the finite upper bound");

    std::cout << "latency histogram tests passed\n";
    return 0;
}