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

fn default_enable_true() -> bool {
    true
}

const TEMPLATE: &str = r#"# 配置模板 (TOML)

# UDP 隧道
[[tunnel]]
protocol = "udp"
listen = "0.0.0.0:7777"
forward = "[2001:db8::1]:8888"
enable = true

# TCP 隧道
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
        Ok(config)
    }
}
