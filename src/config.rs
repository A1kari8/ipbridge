use log::info;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Debug, Deserialize, Serialize)]
pub struct TunnelConfig {
    pub tunnel: Vec<Tunnel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tunnel {
    pub protocol: Protocol,
    /// 角色含义（仅 UDP 需要）
    /// - server: 程序充当服务端部署在靠近游戏客户端一侧，监听 listen，转发到 forward
    /// - client: 程序充当客户端部署在靠近游戏服务器一侧，监听 forward，转发到 listen
    /// 这里的 server/client 是桥所处侧的标记，不是游戏真实的 Server/Client 进程
    #[serde(default)]
    pub role: Option<Role>,
    #[serde(alias = "remote")]
    pub listen: String, // 本地监听地址（对外暴露入口）
    #[serde(alias = "local")]
    pub forward: String, // 转发目标地址（实际要连接的目标，可以是域名）
    #[serde(default = "default_enable_true")]
    pub enable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// 桥在靠近游戏客户端的一侧，对游戏客户端来说像服务端入口
    Server,
    /// 桥在靠近游戏服务器的一侧，对游戏服务器来说像客户端出口
    Client,
}

fn default_enable_true() -> bool {
    true
}

impl TunnelConfig {
    pub fn load_or_create(path: &str) -> Self {
        if !Path::new(path).exists() {
            let template = r#"# 配置模板 (TOML)
#
# 角色说明（仅 UDP 需要）：server/client 描述桥所在的侧，不是游戏真实的 Server/Client
# - server: 程序充当服务端部署在靠近游戏客户端一侧，监听 listen，转发到 forward
# - client: 程序充当客户端部署在靠近游戏服务器一侧，监听 forward，转发到 listen
#
# 协议说明：
# - udp / tcp 均可；udp 支持一对多的 server 会话，client 侧限制单客户端
# - TCP 可省略 role 
#

[[tunnel]]
protocol = "udp"            # 协议：udp / tcp
role = "server"             # server 运行在靠近游戏客户端的一侧
listen = "0.0.0.0:7777"     # server 侧对外监听（给游戏客户端访问的 IPv4:port）
forward  = "[2001:db8::1]:8888" # server 侧流量转发目标（client 侧对外暴露的 IPv6:port）
enable = true

[[tunnel]]
protocol = "udp"
role = "client"             # client 运行在靠近游戏服务器的一侧
listen = "127.0.0.1:54321"  # client 侧本地暴露给游戏服务器的 IPv4:port
forward  = "[2001:db8::1]:8888" # client 侧连接的远端（上面 server 暴露的 IPv6:port）
enable = true

# 如果需要 TCP，可复制上方块，protocol 改为 "tcp"，并可省略 role 字段：
# [[tunnel]]
# protocol = "tcp"
# listen = "0.0.0.0:9000"
# forward  = "10.0.0.5:9000"
# enable = true
"#;

            fs::write(path, template).unwrap();

            info!("Config file not found, generated default config.toml");
            info!("Path: {}", path);
            info!("Please edit it and run again.");
            std::process::exit(0);
        }

        let content = fs::read_to_string(path).expect("读取配置失败");
        let config: TunnelConfig = toml::from_str(&content).expect("解析配置失败");
        config.validate()
    }
}

impl TunnelConfig {
    fn validate(self) -> Self {
        for (idx, t) in self.tunnel.iter().enumerate() {
            if matches!(t.protocol, Protocol::Udp) && t.role.is_none() {
                panic!("tunnel[{}] protocol=udp 需要 role=server/client", idx);
            }
        }
        self
    }
}
