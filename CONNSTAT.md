# Xray connstat 连接监控补丁

> 基于 xray-core **v26.9.9** 官方源码的补丁版：**每条连接的目标域名 + 实时上下行速度 / 累计流量**。
> 数据通过 metrics 端口（HTTP `/debug/vars` 的 `connstat` 键）暴露，可被 [v2rayN connstat 补丁版](https://github.com/bianshuicheng/v2rayN-connstat) 的「Xray 连接」标签页读取，也可用本仓库自带的终端查看器 `connstat-view` 查看。

## 功能特性

- per-connection 统计，计数器名含嗅探域名：`conn>>><id>|<域名>|<入站tag>|<出站tag>>>uplink/downlink`
- TUN 模式开/关都有效（计数挂在内核 dispatcher / dialer 层，TUN 流量同样经过）
- Vision/XTLS 直拷路径不漏计（复用官方预留的 `stat.CounterConnection` 包装，`UnwrapRawConn` 可剥出计数器）
- 计数器命名不匹配 metrics `stats()` 的四段式解析，**不会污染 v2rayN 自身的总速度显示，也不会双重计数**
- 无 `stats`/`metrics` 配置时（NoopManager）自动静默跳过，零开销

## 与官方源码的差异

| 文件 | 改动 |
|---|---|
| `common/connstat/connstat.go` | **新增**。per-connection 计数器的 context 传递 + 连接 ID 分配 |
| `app/dispatcher/connstat.go` | **新增**。在路由决策后注册一对计数器，名字含嗅探域名 |
| `app/dispatcher/default.go` | `routedDispatch` 挂钩：注册计数器 + `context.AfterFunc` 在连接关闭时注销（复用 trackOnlineIP 的清理模式） |
| `transport/internet/dialer.go` | `internet.Dial` 出口把拨号连接包一层 `stat.CounterConnection`（Read=下行 / Write=上行计数） |
| `app/metrics/metrics.go` | `/debug/vars` 新增 `connstat` 键，输出全部活跃连接的结构化 JSON |
| `connstat-view/` | **新增**。终端查看器（HTTP 轮询 metrics / gRPC 双模式） |

其余文件与官方 v26.9.9 完全一致。

## 编译

```bat
go build -o xray-connstat.exe ./main
go build -o connstat-view.exe ./connstat-view
```

## 终端查看器 connstat-view

```text
connstat-view.exe [-url http://127.0.0.1:10812] [-interval 1s] [-hide-inbound api]
connstat-view.exe -api 127.0.0.1:62756      （传统 gRPC StatsService 模式，需配置里有 api 入站）
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `-url` | `http://127.0.0.1:10812` | metrics 端口地址（v2rayN 7.x 默认就是它）。置空 `-url ""` 可关闭该模式 |
| `-api` | 空 | gRPC 模式，与 `-url` 二选一 |
| `-interval` | `1s` | 刷新间隔 |
| `-hide-inbound` | `api` | 隐藏指定入站 tag 的连接（api 控制通道噪音），设空显示全部 |

## 多平台自动构建

本仓库自带上游的 `.github/workflows/release.yml`：**在 GitHub 上发布一个 Release（如 tag `v26.9.9-connstat`）即自动触发全平台编译**（Windows / Linux / macOS / FreeBSD / OpenBSD × amd64 / 386 / arm64 / arm32 等），产物自动挂到该 Release。也可以在 Actions 页面手动 Run workflow。

## 注意事项

- 计数是"线上字节"（含 VLESS/Reality 协议头开销），比客户端侧流量略大几个百分点，属正常。
- Linux 上补丁会使 Vision splice 直拷降级为 readV（仍为内核级 readv，影响很小）；Windows 本来就走 readV，无影响。
- 独立 UDP 协议（hysteria2/tuic 等）走 ListenPacket 的部分暂不计数；vless/vmess/trojan 的 UDP 复用在 TCP 连接里，正常计数。
- 配合 v2rayN 使用时，v2rayN 自动升级内核会覆盖补丁版 `bin\xray\xray.exe`，升级后重新复制补丁版即可。

## 许可

与上游一致：MPL-2.0。补丁改动同样以 MPL-2.0 发布。
