# heartbeatd

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![C++17](https://img.shields.io/badge/C%2B%2B-17-informational.svg)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey.svg)

**高性能 Linux UDP 心跳检测服务** —— 批量系统调用、多 reactor 接收、分片 worker、环形队列、延迟直方图，一个纯 C++17、零第三方依赖的单机原型。

实测（WSL2 loopback，4 个接收 reactor + 4 个 worker，20 秒压测）：**116 万 pps 心跳零丢包，处理延迟 p99 < 500 µs**。

> 这是一个单机原型与压测样本，不是生产级组件。它演示的是"用直白代码把内核批量接口吃满"的工程方法，以及诚实的容量边界：所有数字都标注了机器、内核与配置，不应外推。

## 它解决什么问题

机器需要被快速、便宜地感知存活。TCP 探测有握手开销，ICMP 常被封，轮询日志太慢。`heartbeatd` 用最小的 UDP 报文（32 字节）承载最强的主机身份信息（稳定 host_id + boot_id + 递增序号），在一端检测三类事件：

- **上线**：第一次收到某 host_id 的心跳；
- **重启**：同一 host_id 的 boot_id 变化（进程重启，区别于掉线重连）；
- **超时离线**：窗口内没有更新心跳。

并且能扛量：单机百万级 pps 的持续心跳下识别这些事件，不丢、不抖、可观测。

## 功能特性

- **二进制协议**：魔数 + 版本 + 类型 + host_id 长度 + boot_id + sequence，定长头 24 字节；乱序包按 sequence 过滤。
- **`epoll` + `recvmmsg` 批量接收**：每批最多 64 个数据报，非阻塞排空 socket，高吞吐下系统调用次数相对小批量减半。
- **多接收 reactor（`SO_REUSEPORT`）**：1–64 个接收 reactor 各自持有 socket，按网络流哈希分摊；per-receiver 计数器定位热点。
- **分片 worker**：按稳定 `host_id` 哈希分片，每台主机只由一个 worker 更新，无跨线程锁。
- **`std::list` + `std::unordered_map` 状态表**：类 LRU 顺序 + 平均 O(1) 索引，更新、超时、回滚都是常数摊还。
- **定容环形队列**：启动预分配、按收包批次持锁入队；过载丢新包并计数，绝不挤压内存。
- **内核丢包可观测**：`SO_RXQ_OVFL` 报告 socket 接收队列丢包，与应用队列丢包分开计数。
- **处理延迟直方图**：固定分桶（1 µs ~ 100 ms+），每 worker 输出 p50/p99/max，不用平均值掩盖长尾。
- **本地管理接口**：Unix Domain Socket（0600 权限），`STATS` / `RECEIVER` / `LIST` 三个命令。
- **可选 CPU 绑定**：receiver/worker 固定到指定 CPU，失败只告警不退出（务必实测后启用）。
- **附带工具**：示例客户端、多线程多流压测发送器（预编码，发送端不是瓶颈）、管理查询 CLI。
- **无第三方依赖**：Makefile / CMake 双构建，单元测试 + 冒烟 CI。

## 架构

```
                     UDP datagrams, 32B (magic HBT1 ...)
                                  |
        +-------------------------+-------------------------+
        |            SO_REUSEPORT sockets (1..64)           |
        v                         v                         v
  +-----------+             +-----------+             +-----------+
  | receiver 0|     ...     | receiver r|     ...     | receiver R|   epoll(1 fd)
  +-----+-----+             +-----+-----+             +-----+-----+   recvmmsg x64
        |                         |                         |        MSG_DONTWAIT drain
        |  decode + hash(host_id) % W                        |
        +-------------------------+-------------------------+
                                  |  per-batch enqueue
        +-------------------------+-------------------------+
        v                         v                         v
  +-----------+             +-----------+             +-----------+
  | worker 0  |     ...     | worker w  |     ...     | worker W-1|   fixed ring queue
  +-----+-----+             +-----+-----+             +-----+-----+   single-thread owner
        |                         |                         |
        |  registry: list order + unordered_map index       |
        |  expiry sweep + latency histogram + snapshot      |
        +-------------------------+-------------------------+
                                  v
                     Unix domain admin socket (0600)
                     STATS / RECEIVER / LIST
```

一条心跳的生命周期：socket → `recvmmsg` 批量取回（含 `SO_RXQ_OVFL` 控制消息）→ 校验来源/协议 → `hash(host_id)` 选 worker → 批次入环形队列 → worker 更新状态表（插入失败回滚名额）→ 到期扫描。全程只有两处锁：worker 队列锁与管理快照；接收路径没有全局计数器争抢。

## 快速开始

```bash
# 构建与测试（Linux，C++17，无第三方依赖）
make -j
make test
```

启动服务端：

```bash
./build/heartbeat_server --bind 127.0.0.1 --port 9000 \
  --workers 4 --receivers 4 --timeout-ms 5000 \
  --queue-capacity 16384 --max-hosts 100000 \
  --receive-buffer-bytes 4194304 \
  --allow-source 127.0.0.1 \
  --admin-socket /tmp/heartbeat-admin.sock
```

启动客户端（另一台机器/另一个终端）：

```bash
./build/heartbeat_client --server 127.0.0.1 --port 9000 --id node-42 --interval-ms 1000
```

查询：

```bash
./build/heartbeat_admin --socket /tmp/heartbeat-admin.sock --stats
./build/heartbeat_admin --socket /tmp/heartbeat-admin.sock --receivers
./build/heartbeat_admin --socket /tmp/heartbeat-admin.sock --list 1000
```

CMake（3.16+）同样支持：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

压测（建议独立压测机）：

```bash
./build/heartbeat_bench --server 127.0.0.1 --port 9000 --hosts 10000 \
  --threads 8 --flows 64 --rate-pps 1000000 --duration-sec 20
```

## 压测数据

环境：WSL2 / kernel `6.18.x-microsoft-standard-WSL2`，AMD Ryzen 7 8845H，压测端与服务端同机 IPv4 loopback；`net.core.rmem_max=4,194,304`，`SO_RCVBUF` 请求 64 MiB（实际 8 MiB）。服务端 4 receiver + 4 worker，10,000 host，队列 16,384/worker。

| 目标速率 | 发送线程 | 时长 | 压测端发送数 | 服务端 valid | kernel_dropped | queue_dropped |
|---:|---:|---:|---:|---:|---:|---:|
| 500,000 pps | 4 | 20 s | 9,866,688 | 9,866,688 | 0 | 0 |
| 600,000 pps | 4 | 20 s | 11,735,744 | 11,735,744 | 0 | 0 |
| 800,000 pps | 4 | 20 s | 14,125,088 | 14,125,088 | 0 | 0 |
| 1,000,000 pps | 4 | 20 s | 16,220,352 | 16,220,352 | 0 | 0 |
| 1,000,000 pps | 8 | 20 s | 19,886,816 | 19,886,816 | 0 | 0 |
| 1,100,000 pps | 8 | 20 s | 21,686,656 | 21,686,656 | 0 | 0 |
| 1,200,000 pps | 8 | 20 s | 23,225,344 | 23,225,344 | 0 | 0 |
| 1,300,000 pps | 8 | 20 s | 24,739,520 | 24,644,319 | 95,201 (0.39%) | 0 |

**读法**：目标速率为压测端 pacing 目标，实际发出以 `sent/20s` 折算（如目标 1,200,000 实际约 1,161,000）。发送端 `sent` 是成功交给内核的数量，服务端 `valid` 是真正处理的数量；两者相等即零丢失。4 线程压测端约 81 万 pps 打满，说明彼时瓶颈在发送端；换成 8 线程后才测出服务端 ~116 万 pps 的零丢包点，~124 万 pps 起在最热的接收 socket 上出现 socket 队列丢包——与 `SO_REUSEPORT` 按流哈希不均一致，与处理能力无关（全程 `queue_dropped=0`，p99 延迟仍 < 500 µs）。loopback 数字**不能**外推到物理网卡场景，本仓库 `docs/benchmark-results.md` 记录了完整方法与复现命令。

## 命令行参考

服务端（`heartbeat_server`）：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--bind` / `--port` | 127.0.0.1 / 9000 | 监听地址与 UDP 端口 |
| `--workers` | 4 | worker 线程数（1–256），按 host_id 哈希分片 |
| `--receivers` | 1 | 接收 reactor 数（1–64），>1 时启用 `SO_REUSEPORT` |
| `--queue-capacity` | 8192 | 每 worker 环形队列槽位（总量 ≤ 1,000,000） |
| `--max-hosts` | 100000 | 在线主机上限（全局名额，插入回滚保证不超） |
| `--timeout-ms` | 5000 | 离线判定窗口（100 ms – 24 h） |
| `--receive-buffer-bytes` | 4 MiB | 请求的 `SO_RCVBUF`（内核可能截断，启动日志显示实际值） |
| `--allow-source` | 无 | 来源 IPv4 白名单，可重复 |
| `--admin-socket` | 无 | Unix Domain Socket 管理接口路径（0600） |
| `--cpu-base` | 禁用 | receiver 绑 `[N,N+R)`、worker 绑 `[N+R,N+R+W)`，失败仅告警 |
| `--quiet-events` | 关 | 关闭逐主机上线/离线日志（压测必开） |

`heartbeat_bench`（压测）：`--server/--port/--hosts/--threads/--flows/--rate-pps/--duration-sec`。`--flows` 控制源 socket 数（1–1024），直接影响 `SO_REUSEPORT` 的流分布；为每台模拟主机只编码一次报文，发送前只重写 sequence，发送端不是瓶颈。

`heartbeat_client`（示例）：`--server/--port/--id/--interval-ms`，重启自动生成新 boot_id。

`heartbeat_admin`：`--socket PATH [--stats | --receivers | --list [LIMIT]]`，LIST 上限 10000 条。

## 设计要点

这些决策都可以在代码里直接验证，也是面试/复盘时的论点：

1. **`std::list` + `std::unordered_map` 而不是时间轮/堆**：本场景每个心跳都会刷新时间，堆/时间轮的"删旧插新"是 O(log n)，而链表 splice 是 O(1)；固定超时语义下顺序表头扫到第一个未过期即可停，摊还 O(1)。简单数据结构在正确场景下就是最优的。
2. **环形队列预分配**：启动时一次分配，稳态零分配；过载丢新包并计数，而不是把"队列无限长"当成"高吞吐"。
3. **worker 单次哈希查找 + 回滚**：新主机先插入分片注册表，全局名额校验失败时用 `remove()` 回滚，省掉每包一次哈希查找；注册表由单 worker 独占，无需内部锁。
4. **每批次聚合计数**：接收路径不做逐包全局原子自增，避免多 reactor 在同一个计数器上 CAS 争抢。
5. **区分 `kernel_dropped` 与 `queue_dropped`**：前者是内核 socket 接收队列丢包（调 `SO_RCVBUF`/`rmem_max`），后者是应用队列满（调队列容量或加 worker），排查时方向完全不同。
6. **延迟用直方图而不是均值**：心跳服务的 SLA 是尾延迟；固定分桶 + 溢出桶，读多写少无锁竞争。
7. **在线状态不持久化**：重启后等客户端重新上报，比恢复"可能已过期"的在线状态更安全。
8. **`--cpu-base` 默认关闭**：实测同机混测下绑核反而丢包更多，文档如实记录——优化必须测量。

## 目录结构

```
include/heartbeat/         对外接口（协议、注册表、服务配置）
src/protocol.cpp           协议编解码与 host_id 合法性
src/heartbeat_registry.cpp 主机状态表：list + unordered_map
src/heartbeat_server.cpp   socket 配置、分发循环、管理命令
src/heartbeat_worker.cpp   环形队列、状态更新、超时扫描、延迟直方图
src/heartbeat_receiver.cpp SO_REUSEPORT 接收 reactor
src/latency_histogram.h    固定分桶延迟直方图（header-only）
src/thread_affinity.h      CPU 绑定辅助（header-only）
src/server_log.cpp         线程安全日志
src/admin_socket.cpp       本地管理 socket 服务
src/server_main.cpp        服务端 CLI
src/client_main.cpp        示例客户端
src/benchmark_main.cpp     多线程/多流压测发送器
src/admin_client_main.cpp  管理查询 CLI
tests/                     单元测试（协议 / 注册表 / 直方图）
docs/benchmark-results.md  压测记录（含复现命令）
```

## 测试

```bash
make test          # protocol_test / registry_test / latency_histogram_test
```

覆盖：协议边界（版本、魔数、长度、字符集）、乱序包过滤、重启识别、超时到期、快照截断、名额回滚、直方图分桶与溢出。CI（GitHub Actions）在 ubuntu 上构建、单测并做"服务端 + 压测 + 管理查询"的端到端冒烟。

## 安全、边界与未实现

- 协议**没有密码学认证或加密**。`--allow-source` 是来源 IP 白名单，不是身份认证；不要暴露到不可信公网。跨不可信网络应使用标准密码库实现认证与防重放，不要自研密码算法。
- 管理 socket 仅 Unix 域、0600 权限，不提供公网 HTTP API。
- 单机、单进程、内存态状态；无持久化、无集群复制。
- 物理网卡场景未验证：RSS/RPS、网卡 ring、网卡 offload 的行为与 loopback 不同，请先用压测端确认单接收线程是否为瓶颈，再考虑多 reactor / RSS。
- 已知取舍：环形队列满时丢**新**包；`LIST` 是各分片快照的拼接，持续高负载下不是全局原子快照。

## 路线图

- [ ] 多网卡队列场景的 RSS/RPS 实测报告
- [ ] Prometheus 文本格式的 `/metrics` 输出（沿用现有管理接口）
- [ ] 可选的认证令牌握手（使用标准密码库）
- [ ] 线程/队列级 CPU profile 采样

## License

[MIT](LICENSE)。欢迎 issue / PR；数据不造假，边界写清楚，是这个仓库的规矩。