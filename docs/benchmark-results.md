# 压测记录（WSL2 loopback）

记录日期：2026-10-09。此结果只对应下述测试环境和配置，不是生产容量承诺。

## 环境与配置

- WSL2 / Linux kernel `6.18.33.2-microsoft-standard-WSL2`
- AMD Ryzen 7 8845H，WSL 可见 16 个逻辑 CPU；可见内存约 7.4 GiB
- 客户端压测器和服务端在同一 WSL 实例，通过 IPv4 loopback 通信
- 服务端：1 个 receiver、4 个 worker、10,000 个模拟主机、队列容量 16,384/worker、最大主机 20,000、事件日志关闭
- `net.core.rmem_max=4,194,304`，`SO_RCVBUF` 请求 64 MiB，`getsockopt` 返回 8 MiB
- 压测器：4 个发送线程、`sendmmsg`、目标速率按表设置

## 结果

| 目标速率 | 实际发送速率 | 时长 | 压测器发送数 | 服务端 valid | kernel_dropped | queue_dropped |
|---:|---:|---:|---:|---:|---:|---:|
| 200,000 pps | 199,901 pps | 3 s | 599,872 | 599,872 | 0 | 0 |
| 250,000 pps | 249,800 pps | 20 s | 4,996,224 | 4,996,224 | 0 | 0 |
| 300,000 pps | 299,554 pps | 20 s | 5,991,328 | 5,991,094 | 234 | 0 |
| 350,000 pps | 348,656 pps | 20 s | 6,973,344 | 6,834,906 | 138,438 | 0 |
| 400,000 pps | 398,007 pps | 5 s | 1,990,336 | 1,725,107 | 264,719 | 0 |

250,000 pps 在这次 20 秒测试中没有观察到内核或应用队列丢包；300,000 pps 开始出现少量 socket 接收队列丢包，350,000 pps 丢包明显增加。因此，对**这台 WSL2 测试配置**，可以把 250,000 pps 视为本次测得的无观测丢包点；若需要留运行余量，可先按约 200,000 pps 做初始容量规划，再在目标机器上重复长时间压测。

## 复现方式

先启动服务端：

```bash
./build/heartbeat_server --bind 127.0.0.1 --port 9000 --workers 4 --timeout-ms 10000 --queue-capacity 16384 --max-hosts 20000 --receive-buffer-bytes 67108864 --allow-source 127.0.0.1 --quiet-events
```

另一个终端启动压测器：

```bash
./build/heartbeat_bench --server 127.0.0.1 --port 9000 --hosts 10000 --threads 4 --rate-pps 250000 --duration-sec 20
```

压测器的 `sent` 是发送端成功交给内核的数量。关停服务端后比较其最终 `valid`、`kernel_dropped` 和 `queue_dropped`。不同机器、内核、`rmem_max`、CPU 负载、日志模式和网卡路径都会改变结果；loopback 结果不能代替跨网卡压测。




## SO_REUSEPORT 多 reactor 对比

同一台 WSL2 环境、同样的 4 个 worker、10,000 个 host ID、`SO_RCVBUF` 请求 64 MiB（每个 socket 实际 8 MiB）、队列 16,384/worker；服务端改用 4 个 receiver reactor。压测端使用 4 个发送线程，并通过 `--flows 64` 打开 64 个稳定的 UDP 源流，使每个 host ID 始终从同一源 socket 发包。

| 目标速率 | 时长 | 压测器发送数 | 服务端 valid | kernel_dropped | queue_dropped |
|---:|---:|---:|---:|---:|---:|
| 350,000 pps | 20 s | 6,962,880 | 6,962,880 | 0 | 0 |
| 500,000 pps | 20 s | 9,796,032 | 9,796,032 | 0 | 0 |
| 600,000 pps | 20 s | 11,154,560 | 10,917,388 | 234,574 | 0 |

该次测试显示，4 个 SO_REUSEPORT reactor 在 500,000 pps 下 20 秒没有观察到丢包，600,000 pps 时 socket 丢包约 2.1%。接收端逐 socket 统计显示，600,000 pps 时流分配并不均匀：其中一个接收 socket 承担了更高流量并出现丢包。

SO_REUSEPORT 按网络流哈希选择 socket，不是逐数据报轮询；发送源流数少或哈希结果倾斜时，增加 receiver 数不一定提升容量，甚至会出现部分 socket 空闲、另一个 socket 过载。`--flows` 用来控制压测器源 socket 数；真实部署仍要用真实主机 IP/端口分布验证。

## 第三轮：热路径优化后的最终数据

第三轮在 SO_REUSEPORT 多 reactor 基础上继续做了三件事：

1. **压测器预编码**：每台模拟主机的数据报只编码一次（boot_id 和 host_id 不变），发送前只重写协议头里的 sequence 8 个字节。这消除了发送端每包的字符串构造和合法性检查，让发送端不再是瓶颈，服务端被测到真实上限。
2. **接收批量 32 → 64**：`recvmmsg` 每批 64 个数据报，高吞吐下系统调用次数减半。
3. **worker 单次哈希查找**：新主机先插入分片注册表，再做全局名额校验，失败回滚（`HeartbeatRegistry::remove`），去掉原来的 `contains + record` 两次查找。与上一轮的"每批次聚合计数、去掉接收路径全局原子"叠加，接收线程和 worker 线程内部都不再有跨核争抢的共享计数。

配置与上文 SO_REUSEPORT 对比相同：4 个 receiver、4 个 worker、10,000 个 host ID、队列 16,384/worker、`SO_RCVBUF` 请求 64 MiB（实际 8 MiB）、服务端与压测端同机 loopback、`net.core.rmem_max=4,194,304`。压测端 4 个发送线程、`--flows 64`。

| 目标速率 | 时长 | 压测器发送数 | 服务端 valid | kernel_dropped | queue_dropped |
|---:|---:|---:|---:|---:|---:|
| 500,000 pps | 20 s | 9,866,688 | 9,866,688 | 0 | 0 |
| 600,000 pps | 20 s | 11,735,744 | 11,735,744 | 0 | 0 |
| 700,000 pps | 20 s | 13,339,584 | 13,339,584 | 0 | 0 |
| 800,000 pps | 20 s | 14,125,088 | 14,125,088 | 0 | 0 |
| 1,000,000 pps | 20 s | 16,220,352 | 16,220,352 | 0 | 0 |

也就是说，在**这台 WSL2、这一组配置**下，发送端实际送出约 811,000 pps 时服务端 20 秒零丢包；处理延迟 p50 约 50–250 µs、p99 约 250–500 µs（由 worker 的固定分桶直方图统计）。第二轮 600,000 pps 目标下观察到的 2.1% socket 丢包，在本轮没有复现——预编码发送端把实际吞吐推高的同时，接收路径改动也移除了每包两次哈希查找的开销。把压测线程从 4 加到 8 之后，发送端可以稳定送出更高压力，服务端到 116 万 pps 仍然零丢包（见下一节）。

### 上限探测（同样是同机 loopback）

上表的 4 线程压测端在 1,000,000 pps 目标时只能实际发出 81 万，说明瓶颈在发送端。换成 8 个发送线程（其余不变）继续加压，逐级找到服务端的真实拐点：

| 目标速率 | 实际发送速率 | 压测器发送数 | 服务端 valid | kernel_dropped | queue_dropped |
|---:|---:|---:|---:|---:|---:|
| 1,000,000 pps | 994,260 pps | 19,886,816 | 19,886,816 | 0 | 0 |
| 1,100,000 pps | 1,084,250 pps | 21,686,656 | 21,686,656 | 0 | 0 |
| 1,200,000 pps | 1,161,160 pps | 23,225,344 | 23,225,344 | 0 | 0 |
| 1,300,000 pps | 1,236,900 pps | 24,739,520 | 24,644,319 | 95,201 (0.39%) | 0 |

即在**这台 WSL2、这一组配置**下，服务端 20 秒零丢包的实测吞吐达到 **116 万 pps**（4 个 receiver、4 个 worker、64 个源流）；到 124 万 pps 才开始在最热的接收 socket 上出现 0.39% 的 socket 接收队列丢包。延迟方面，116 万 pps 时 p50 仍为 50–250 µs、p99 为 250–500 µs、最大 5–20 ms，说明队列没有被堆积拖垮，丢包只发生在内核 socket 层。

同一目标速率下另有两组对照：

| 配置 | 目标速率 | 实际发送速率 | 服务端 valid | kernel_dropped | 说明 |
|---|---:|---:|---:|---:|---|
| 4 receivers + `--cpu-base 0` | 1,300,000 pps | 1,232,300 pps | 23,593,459 | 1,054,765 (4.3%) | 同机混测下绑核反而更差 |
| 8 receivers | 1,300,000 pps | 1,148,600 pps | 22,974,464 | 0 | 流分散后零丢包，但发送端反而只剩 1.15M pps |

三点结论，都可以直接写进工程判断：

- 单机 loopback 下，4 receiver 的服务端接收侧零丢包上限约在 **116 万 pps**，超过后先在**最热的接收 socket**上丢包，这与 SO_REUSEPORT 按流哈希、不保证均匀一致。
- `--cpu-base` 在有独立压测机、且 traffic 经过物理网卡的场景才是常规优化；在压测端和服务端抢同一批 CPU 的 WSL2 loopback 测试里，绑核把 softirq 和 worker 挤到同一批核上，丢包反而涨了一个数量级。**绑核必须实测，不能默认打开。**
- 流数量比 receiver 数量更影响丢包分布：64 个源流 + 8 个 receiver 时零丢包，而 64 个源流 + 4 个 receiver 时热点 socket 先满。

### 复现本轮数据

服务端（-j4 并发编译后）：

```bash
./build/heartbeat_server --bind 127.0.0.1 --port 9000 --workers 4 --receivers 4 \
  --timeout-ms 10000 --queue-capacity 16384 --max-hosts 20000 \
  --receive-buffer-bytes 67108864 --allow-source 127.0.0.1 --quiet-events
```

压测端（4 线程 64 流）：

```bash
./build/heartbeat_bench --server 127.0.0.1 --port 9000 --hosts 10000 \
  --threads 4 --flows 64 --rate-pps 800000 --duration-sec 20
```

对比发送端 `sent` 与服务端最终 `valid`/`kernel_dropped`/`queue_dropped`；`shard` 行里的 `latency_p50_us/latency_p99_us/latency_max_us` 是处理延迟的桶近似值。所有数字只对上述机器、内核和配置有效，不应外推。