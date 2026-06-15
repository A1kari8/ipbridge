# ipbridge

极简 TCP/UDP 隧道转发工具，支持 IPv4/IPv6 双栈桥接

## 场景

在协议栈不同的环境间搭建隧道。典型场景：

- 程序只支持 IPv4，服务器却只拥有 IPv6 公网地址
- 两端没有公网 IP，需要通过中间节点桥接
- 需要将本地的 TCP/UDP 服务暴露到另一网络

## 功能

- **TCP 隧道**：透明双向转发，支持多并发连接
- **UDP 隧道**：无状态转发，支持 Server/Client 双角色
  - Server 模式：靠近游戏客户端一侧，支持多客户端会话（60s 超时清理）
  - Client 模式：靠近游戏服务器一侧，限制单客户端确保响应路由正确
- **连通性检测**：`ipbridge check` 验证配置并测试远程端是否存活
- **双栈支持**：IPv4 / IPv6 地址任意组合
- **配置驱动**：TOML 配置文件，`ipbridge init` 自动生成模板

## 使用

### 子命令

| 命令 | 说明 |
|------|------|
| `ipbridge init` | 生成配置模板 |
| `ipbridge init -f` | 强制覆盖已有配置 |
| `ipbridge check` | 检查配置是否可用 |
| `ipbridge` | 运行代理（默认） |
| `ipbridge run` | 同上，显式运行 |

### 配置

配置文件示例（`ipbridge init` 自动生成）：

```toml
[[tunnel]]
protocol = "udp"
role = "server"
listen = "0.0.0.0:7777"
forward = "[2001:db8::1]:8888"
enable = true

[[tunnel]]
protocol = "udp"
role = "client"
listen = "127.0.0.1:54321"
forward = "[2001:db8::1]:8888"
enable = true

# [[tunnel]]
# protocol = "tcp"
# listen = "0.0.0.0:9000"
# forward = "10.0.0.5:9000"
# enable = true
```

参数说明：

| 字段 | 说明 |
|------|------|
| `protocol` | `tcp` 或 `udp` |
| `role` | UDP 时必填：`server` 或 `client` |
| `listen` | 本地监听地址 |
| `forward` | 转发目标地址（支持域名） |
| `enable` | 是否启用该隧道，默认 `true` |

## License

MIT
