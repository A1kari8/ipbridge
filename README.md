# ipbridge

极简 TCP/UDP 隧道转发工具，支持 IPv4/IPv6 双栈桥接

## 场景

在协议栈不同的环境间搭建隧道。典型场景：

- 程序只支持 IPv4，服务器却只拥有 IPv6 公网地址

## 功能

- **TCP 隧道**：透明双向转发，支持多并发连接
- **UDP 隧道**：NAT session 管理、自适应超时
- **链路压缩**：UDP/TCP 逐包/逐块压缩，支持 zlib / gz / zstd
- **连通性检测**：`ipbridge check` 验证配置并测试远程端是否存活
- **双栈支持**：IPv4 / IPv6 地址任意组合
- **配置驱动**：TOML 配置文件，`ipbridge init` 生成模板

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
listen = "[::]:7777"
forward = "127.0.0.1:8888"
enable = true

# [[tunnel]]
# protocol = "tcp"
# listen = "0.0.0.0:9000"
# forward = "10.0.0.5:9000"
# enable = true
```

### 常见部署示例

#### 服务端用户

```toml
[[tunnel]]
protocol = "udp"
listen = "[::]:7777"
forward = "<服务端应用监听地址>" # 例：127.0.0.1:8888
enable = true
```

#### 客户端用户

```toml
[[tunnel]]
protocol = "udp"
listen = "0.0.0.0:7777"
forward = "<服务端公网地址>:7777"
enable = true
```

客户端应用连接时填写 `127.0.0.1:7777` 即可，`ipbridge` 会双向转发数据

### 参数说明

| 字段 | 说明 |
|------|------|
| `protocol` | `tcp` 或 `udp` |
| `listen` | 本地监听地址 |
| `forward` | 转发目标地址（支持域名） |
| `enable` | 是否启用该隧道，默认 `true` |
| `compress` | 压缩算法：`zlib`、`gz` 或 `zstd`（可带等级，如 `"zstd:3"`），默认不压缩 |

### 链路压缩

压缩作用于两台 `ipbridge` 之间的链路段，普通应用与服务端收到的始终是原始数据。开启压缩的隧道会压缩自己发往链路的包，并对接收的包**自动检测是否压缩并解压**：

```toml
# 客户端
[[tunnel]]
protocol = "udp"
listen = "0.0.0.0:7777"
forward = "<服务端公网地址>:7777"
compress = "zstd:3"   # zlib/gz 等级 1-9，zstd 等级 1-22；省略等级用默认

# 服务端
[[tunnel]]
protocol = "udp"
listen = "[::]:7777"
forward = "127.0.0.1:8888"
compress = "zstd"
```

行为说明：

- **UDP**：任意一端开启压缩，该端发往链路的包即被压缩；对端（即使未开启压缩）会自动识别并解压。因此"只有客户端开"或"只有服务端开"都能正常传输。
- **TCP**：压缩需要在链路两端同时开启（TCP 为字节流，单边开启无法正确解码）。
- 压缩对已压缩/加密数据自动回退为原始传输，不会越压越大；两端可用不同算法/等级互通。
- 所有压缩帧均带协议魔数，接收端据此自动识别，不影响普通明文数据。

## License

MIT
