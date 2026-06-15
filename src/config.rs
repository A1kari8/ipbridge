use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Debug, Deserialize, Serialize)]
pub struct TunnelConfig {
    pub tunnel: Vec<Tunnel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tunnel {
    pub protocol: Protocol,
    #[serde(default)]
    pub role: Option<Role>,
    pub listen: String,
    pub forward: String,
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
    Server,
    Client,
}

fn default_enable_true() -> bool {
    true
}

const TEMPLATE: &str = r#"# 配置模板 (TOML)
#
# 角色说明（仅 UDP 需要）：
# - server: 桥在靠近游戏客户端一侧，监听 listen，转发到 forward
# - client: 桥在靠近游戏服务器一侧，监听 listen，转发到 forward
#
# 协议说明：
# - udp / tcp 均可；udp 支持一对多的 server 会话，client 侧限制单客户端
# - TCP 可省略 role

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

# 如果需要 TCP，可复制上方块，protocol 改为 "tcp"，并可省略 role 字段：
# [[tunnel]]
# protocol = "tcp"
# listen = "0.0.0.0:9000"
# forward = "10.0.0.5:9000"
# enable = true
"#;

impl TunnelConfig {
    pub fn create_template(path: impl AsRef<Path>, force: bool) -> Result<()> {
        let path = path.as_ref();
        if path.exists() && !force {
            anyhow::bail!(
                "{} already exists. Use --force to overwrite.",
                path.display()
            );
        }
        fs::write(path, TEMPLATE)
            .with_context(|| format!("Failed to write template to {}", path.display()))?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path).with_context(|| {
            format!(
                "Failed to read config file {}. Use `ipbridge init` to generate a template.",
                path.display()
            )
        })?;
        let config: TunnelConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        for (idx, t) in self.tunnel.iter().enumerate() {
            if matches!(t.protocol, Protocol::Udp) && t.role.is_none() {
                anyhow::bail!(
                    "tunnel[{}]: protocol=udp requires role=server or client",
                    idx
                );
            }
        }
        Ok(())
    }
}
