# 相较官方 Xray-core 的修改说明

本仓库基于官方 Xray-core（内核基线 26.9.30），在官方代码之上叠加了以下修改。全部修改在 git 历史中可查，本文按功能域归纳；涉及的主要源码文件随条目标注。

---

## 一、connstat 连接监控（面板数据源）

为「Xray 连接」面板提供按连接/按应用的实时数据：

- 每条连接的**目标域名/IP、实时上下行速率、累计流量、存活时长、协议识别**（嗅探结果）
- **进程识别**：按入站源 `ip:port` 反查系统 socket 表，得到进程名/PID/可执行路径；内核自身流量明确标注
- 数据暴露在 metrics 端口 `/debug/vars` 的 `flowwatch` / `appwatch` / `connstat` 键，供 GUI 每秒轮询

主要文件：`common/flowwatch/`（流表、速率窗口、归属查询）、`common/net/find_process_cache_windows.go`（socket 表共享缓存，按进程分流与识别共用）、`app/metrics/`。

## 二、TUN 模式增强

### 2.1 防自环
- `proxy/tun/egress_windows.go`：出站拨号前查系统路由，目的地若经自家 TUN 则拒绝拨号
- `common/net/self_outbound.go`：出站 socket 登记表 + TUN 地址注册表，双向识别自环流量

### 2.2 断网风暴治理（对齐 mihomo 的断网行为）
- `proxy/tun/reap.go`：连接收割器
  - 僵尸连接回收（发起后 5s 无应答 / 30s 无活动）
  - **空闲回收**：TCP 空闲 300s、UDP 空闲 60s 回收（原版连接永不主动回收）
  - 并发上限 1024，超限拒绝
- **断网熔断**：检测到出口接口不可用时，准入完全关闭
  - TCP 在 **SYN 阶段直接 RST**（不建 gVisor 端点、不起协程、不配缓冲）
  - UDP 包直接丢弃（不建会话与缓冲通道）
  - 恢复为**事件驱动**：出口接口重新出现即刻开门；30s 硬过期兜底
- 运行计数器：`/debug/vars` 的 `tunloop`（`reaped` / `over_cap` / `live` / `offline_refused` / `uplink_refused` / `breaker_refused` / `route_refused`）

### 2.3 出站接口管理
- `proxy/tun/config.go`：InterfaceUpdater 记录出口接口变化时间戳，供熔断恢复与拨号决策使用

## 三、稳定性修复（相较官方的行为修正）

| 位置 | 官方行为 | 修改后 |
|---|---|---|
| `proxy/vless/outbound/outbound.go` | testpre 预连接失败后**零退避死循环**（断网 CPU 满载 + 日志洪水） | 500ms 退避、日志 5s 节流、等待 10s 上限后落到普通拨号 |
| `proxy/dns/dns.go` | DNS 会话存在**永久泄漏**（task.Run 孤儿任务 + 空闲定时器被禁用）；会话空闲上限 5 分钟 | 任一侧结束即回收整个会话；空闲上限 30s；断网时查询立即 SERVFAIL |
| `main/run.go` | 无周期性内存归还 | 每分钟 `debug.FreeOSMemory()`，断网期推高的堆水位主动归还系统 |
| `proxy/tun/handler.go` | 每条失败连接逐条刷错误日志 | 30s 节流汇总，避免拖垮接管 stdout 的 GUI |
| `common/net/find_process_cache_windows.go` | 进程归属查不到即放弃（短命连接大量未识别） | 快照历史回溯（最近两代）+ 未命中即重建（120ms 限流）+ flowwatch 120ms/400ms 提前重查 |

## 四、其他

- `app/dns/negative_cache.go`：DNS 负缓存，减少重复失败解析的开销
- 按进程分流的共享 socket 表缓存（进程识别与按进程分流共用，避免风暴期逐连接枚举系统表）
- `app/dispatcher/default.go`：嗅探缓冲从 32KiB 降到 8KiB（降低风暴期每连接内存驻留）

## 五、与官方的行为差异提示

1. **空闲连接会被回收**（TCP 300s / UDP 60s 无活动）——官方永不回收。超长空闲的连接（如挂起的 SSH）会被断开
2. **断网期间**：外网目标的连接在门口即被拒绝（不进管线），恢复后立即正常；内网/私有目标当前同样被拦（如需断网期访问内网，可恢复历史提交 `652b267` 的私有目标豁免）
3. **DNS 会话** 30s 空闲上限（官方 5 分钟）；断网时域名查询立即返回失败而非挂起
4. 启用 `testpre` 时行为与官方不同（退避重试而非死循环）

---

配套 GUI：[v2rayN-connstat](https://github.com/bianshuicheng/v2rayN-connstat)（「Xray 连接」标签页读取本内核的 connstat 数据）。
