# Xray-core-connstat

**带连接监控功能的 Xray 内核** —— 基于 [XTLS/Xray-core](https://github.com/XTLS/Xray-core) v26.9.9 补丁修改。

原版 Xray 内核只能看到**总速度 / 总流量**，看不到每一条连接在干什么。本补丁给内核加上了 **connstat 连接监控**：每一条代理连接的**目标域名、实时上下行速度、累计流量、存活时长**，全部实时可见。

> 图形界面版请配合 → [v2rayN-connstat](https://github.com/bianshuicheng/v2rayN-connstat)（独立的「Xray 连接」标签页）

---

## ✨ 新功能：connstat 连接监控

| 能力 | 说明 |
|---|---|
| **按连接统计** | 每条连接独立计数，计数器名带嗅探出的目标域名：`conn>>><id>|<域名>|<入站tag>|<出站tag>>>uplink/downlink` |
| **进程识别** | 连接建立时按入站源 `ip:port` 异步反查系统 socket 表，得到发起连接的**进程名、PID、可执行文件路径**（独立 goroutine 不阻塞转发，连接关闭自动清理） |
| **实时速度 + 累计流量** | 上行/下行分开统计，既算瞬时速率也算累计字节 |
| **TUN 模式可用** | 计数挂在内核 dispatcher / dialer 层，TUN 开或关都能统计到 |
| **直拷路径不漏计** | 复用官方预留的 `stat.CounterConnection` 包装，Vision/XTLS 直拷（raw copy）路径同样计入 |
| **不污染原有统计** | 计数器命名独立于 metrics `stats()` 四段式解析，不会让 v2rayN 的总速度翻倍或错乱 |
| **零开销旁路** | 配置里没有 `stats`/`metrics` 时自动跳过，不产生任何额外负担 |

数据通过 metrics 端口（HTTP `/debug/vars` 的 `connstat` 键）以 JSON 输出，任何工具都能读取。

## 🖥️ 配套终端查看器 connstat-view

不装 v2rayN 也能用，本仓库 Release 附带：

```text
connstat-view.exe [-url http://127.0.0.1:10812] [-interval 1s] [-hide-inbound api]
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `-url` | `http://127.0.0.1:10812` | metrics 端口（v2rayN 7.x 默认就是它），置空 `-url ""` 关闭该模式 |
| `-api` | 空 | 传统 gRPC StatsService 模式（需配置里有 api 入站），与 `-url` 二选一 |
| `-interval` | `1s` | 刷新间隔 |
| `-hide-inbound` | `api` | 隐藏指定入站 tag（过滤 api 控制通道噪音），设空显示全部 |

## 📥 下载

到 [Releases](https://github.com/bianshuicheng/xray-core-connstat/releases) 下载：

- `xray-connstat.exe` —— Windows x64 补丁版内核
- `connstat-view.exe` —— 终端连接查看器
- 其余平台包（Linux / macOS / BSD × amd64 / 386 / arm64 / arm32 等）由 GitHub Actions 在发布时自动编译并补充到同一 Release

## 🚀 快速开始（配合 v2rayN）

1. 下载 [v2rayN-connstat](https://github.com/bianshuicheng/v2rayN-connstat)（界面里就是「Xray 连接」标签页）
2. 用 `xray-connstat.exe` 替换 v2rayN 目录下的 `bin\xray\xray.exe`
3. 重启 v2rayN → 打开「Xray 连接」标签页，每秒刷新的连接监控就在那里

## 🔧 与原版的区别

| 文件 | 改动 |
|---|---|
| `common/connstat/connstat.go` | **新增**。per-connection 计数器的 context 传递 + 连接 ID 分配 + 进程信息（process/pid/path）登记 |
| `app/dispatcher/connstat.go` | **新增**。路由决策后注册一对计数器（名字含嗅探域名）+ 异步进程反查（入站源 `ip:port` → 系统进程） |
| `app/dispatcher/default.go` | `routedDispatch` 挂钩：注册计数器、触发进程识别 + `context.AfterFunc` 在连接关闭时注销 |
| `transport/internet/dialer.go` | `internet.Dial` 出口给拨号连接包一层 `stat.CounterConnection`（Read=下行 / Write=上行） |
| `app/metrics/metrics.go` | `/debug/vars` 新增 `connstat` 键，输出全部活跃连接的结构化 JSON（含 process/pid/path 字段） |
| `connstat-view/` | **新增**。终端查看器（HTTP metrics / gRPC 双模式，含进程列） |
| `README.md` / `CONNSTAT.md` | **新增/替换**。本补丁的说明文档（原版说明在 `README-upstream.md`） |

**除上述文件外，与官方 v26.9.9 源码完全一致。**

## 🛠️ 从源码编译

```bat
go build -o xray-connstat.exe ./main
go build -o connstat-view.exe ./connstat-view
```

## 🤖 多平台自动构建

本仓库保留了上游的 Actions 工作流：**发布一个 Release（任意 tag）即自动触发全平台编译**，产物自动挂到该 Release；也可以在 Actions 页面手动 Run workflow。

## 📝 更新记录

- **2026-09-27（内核 v3）**
  - **TUN 全新启动后进程名集体消失的根因修复**：上游 `IsLocal()` 把本机接口地址列表缓存 60 秒，TUN 网卡的 IP 往往还没进缓存，首个连接起一分钟内所有 TUN 源地址被误判为"非本机"直接拒绝查询。补丁移除了这个前置门——反查的源地址本来就是本机入站收到的，真正非本机的源在系统 socket 表里同样匹配不到行，结果不变但不再被过期缓存误杀。
  - **查询失败不再静默**：首查未命中时以 300ms / 1s / 3s 在后台重试三次（UDP 立发即关的套接字等边缘情况），最终失败登记 `LOOKUP-FAILED: <原因>`；metrics `connstat` 条目新增 `src` 字段（`network srcIP:srcPort -> dstIP:dstPort`），失败原因可直接从接口排查。
  - **wintun 适配器优先复用**：`open()` 先尝试 `OpenAdapter`，失败才 `CreateAdapter`。后者要走设备安装流程，其中的私有命名空间互斥锁在内核以 SYSTEM 服务身份运行时会被 DACL 拒绝。
- **2026-09-27（内核 v2 修订）**：**进程识别改为同步查询**——原实现在独立 goroutine 里异步反查 socket 表，极短命连接在查询完成前就已关闭，导致进程列留空；现在在连接建立的 dispatch goroutine 上直接查询，保证查询时 socket 仍然存活，短命连接也能正确显示进程。内核自身发起的连接（DNS 模块）没有客户端 socket，明确保持无进程名。
- **2026-09-26（内核 v2）**：新增进程识别（按入站源 `ip:port` 反查系统 socket 表，登记 process/pid/path）。
- **2026-09-26（内核 v1）**：connstat 连接监控补丁（per-connection 域名/实时速度/累计流量）。

## ⚠️ 注意事项

- 计数是"线上字节"（含 VLESS/Reality 协议头开销），比客户端侧流量略大几个百分点，属正常。
- **进程识别的边界**：进程查询为同步执行 + 后台重试三次（连接建立时 socket 必然存活，短命连接也能查到；查不到的在 300ms/1s/3s 后重试，最终失败显示 `LOOKUP-FAILED`）；内核自身发起的连接（DNS 模块）没有客户端 socket，无进程名；局域网来源、部分 UWP 应用可能查不到。
- Linux 上补丁会使 Vision splice 直拷降级为 readV（仍为内核级 readv，影响很小）；Windows 本来就走 readV，无影响。
- 独立 UDP 协议（hysteria2/tuic 等）走 ListenPacket 的部分暂不计数；vless/vmess/trojan 的 UDP 复用在 TCP 连接里，正常计数。
- 配合 v2rayN 时，v2rayN 自动升级内核会覆盖补丁版 `bin\xray\xray.exe`，升级后重新复制补丁版即可。

## 🙏 致谢与许可

- 上游项目：[XTLS/Xray-core](https://github.com/XTLS/Xray-core)
- 图形界面：[v2rayN-connstat](https://github.com/bianshuicheng/v2rayN-connstat)
- 许可证与上游一致：[MPL-2.0](LICENSE)，本补丁改动同样以 MPL-2.0 发布
