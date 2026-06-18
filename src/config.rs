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
listen = "[::]:7777"
forward = "127.0.0.1:8888"
enable = true

# TCP 隧道
# [[tunnel]]
# protocol = "tcp"
# listen = "0.0.0.0:9000"
# forward = "10.0.0.5:9000"
# enable = true

# 端口范围映射
# [[tunnel]]
# protocol = "udp"
# listen = "[::]:7777-7780"
# forward = "127.0.0.1:8888-8891"
# enable = true
"#;

fn split_addr_port(s: &str) -> Result<(&str, &str)> {
    if s.starts_with('[') {
        let idx = s
            .rfind("]:")
            .ok_or_else(|| anyhow::anyhow!("Invalid IPv6 address format: {}", s))?;
        Ok((&s[..idx + 1], &s[idx + 2..]))
    } else {
        let idx = s
            .rfind(':')
            .ok_or_else(|| anyhow::anyhow!("Missing port in address: {}", s))?;
        Ok((&s[..idx], &s[idx + 1..]))
    }
}

fn parse_port_range(s: &str) -> Result<(u16, u16)> {
    if let Some(idx) = s.find('-') {
        let start = s[..idx].parse()?;
        let end = s[idx + 1..].parse()?;
        if start > end {
            anyhow::bail!("Port range start > end: {}", s);
        }
        Ok((start, end))
    } else {
        let port = s.parse()?;
        Ok((port, port))
    }
}

fn expand_port_ranges(config: &mut TunnelConfig) -> Result<()> {
    let mut expanded = Vec::new();
    for tunnel in &config.tunnel {
        let (listen_base, listen_port) = split_addr_port(&tunnel.listen)?;
        let (forward_base, forward_port) = split_addr_port(&tunnel.forward)?;

        let has_lr = listen_port.contains('-');
        let has_fr = forward_port.contains('-');

        if !has_lr && !has_fr {
            expanded.push(tunnel.clone());
            continue;
        }

        if has_lr != has_fr {
            anyhow::bail!(
                "Port range must be specified on both listen and forward: {} -> {}",
                tunnel.listen,
                tunnel.forward
            );
        }

        let (l_start, l_end) = parse_port_range(listen_port)?;
        let (f_start, f_end) = parse_port_range(forward_port)?;

        let l_count = l_end - l_start + 1;
        let f_count = f_end - f_start + 1;

        if l_count != f_count {
            anyhow::bail!(
                "Port range count mismatch: {} ports in listen, {} in forward",
                l_count,
                f_count
            );
        }

        for i in 0..l_count {
            expanded.push(Tunnel {
                listen: format!("{}:{}", listen_base, l_start + i),
                forward: format!("{}:{}", forward_base, f_start + i),
                protocol: tunnel.protocol.clone(),
                enable: tunnel.enable,
            });
        }
    }
    config.tunnel = expanded;
    Ok(())
}

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
        let mut config: TunnelConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file {}", path.display()))?;
        for t in &mut config.tunnel {
            t.listen = t.listen.trim().to_string();
            t.forward = t.forward.trim().to_string();
        }
        expand_port_ranges(&mut config)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_addr_port_ipv4() {
        let (addr, port) = split_addr_port("127.0.0.1:8888").unwrap();
        assert_eq!(addr, "127.0.0.1");
        assert_eq!(port, "8888");
    }

    #[test]
    fn split_addr_port_ipv4_range() {
        let (addr, port) = split_addr_port("0.0.0.0:9000-9005").unwrap();
        assert_eq!(addr, "0.0.0.0");
        assert_eq!(port, "9000-9005");
    }

    #[test]
    fn split_addr_port_ipv6() {
        let (addr, port) = split_addr_port("[::]:7777").unwrap();
        assert_eq!(addr, "[::]");
        assert_eq!(port, "7777");
    }

    #[test]
    fn split_addr_port_ipv6_range() {
        let (addr, port) = split_addr_port("[::]:7777-7780").unwrap();
        assert_eq!(addr, "[::]");
        assert_eq!(port, "7777-7780");
    }

    #[test]
    fn parse_single_port() {
        let (start, end) = parse_port_range("7777").unwrap();
        assert_eq!(start, 7777);
        assert_eq!(end, 7777);
    }

    #[test]
    fn parse_port_range_valid() {
        let (start, end) = parse_port_range("7777-7780").unwrap();
        assert_eq!(start, 7777);
        assert_eq!(end, 7780);
    }

    #[test]
    fn parse_port_range_invalid_order() {
        assert!(parse_port_range("7780-7777").is_err());
    }

    #[test]
    fn expand_range_expands_correctly() {
        let mut config = TunnelConfig {
            tunnel: vec![Tunnel {
                protocol: Protocol::Udp,
                listen: "[::]:7777-7779".into(),
                forward: "127.0.0.1:8888-8890".into(),
                enable: true,
            }],
        };
        expand_port_ranges(&mut config).unwrap();
        assert_eq!(config.tunnel.len(), 3);
        assert_eq!(config.tunnel[0].listen, "[::]:7777");
        assert_eq!(config.tunnel[0].forward, "127.0.0.1:8888");
        assert_eq!(config.tunnel[1].listen, "[::]:7778");
        assert_eq!(config.tunnel[1].forward, "127.0.0.1:8889");
        assert_eq!(config.tunnel[2].listen, "[::]:7779");
        assert_eq!(config.tunnel[2].forward, "127.0.0.1:8890");
    }

    #[test]
    fn expand_single_port_no_change() {
        let mut config = TunnelConfig {
            tunnel: vec![Tunnel {
                protocol: Protocol::Tcp,
                listen: "0.0.0.0:9000".into(),
                forward: "10.0.0.5:9000".into(),
                enable: true,
            }],
        };
        expand_port_ranges(&mut config).unwrap();
        assert_eq!(config.tunnel.len(), 1);
        assert_eq!(config.tunnel[0].listen, "0.0.0.0:9000");
    }

    #[test]
    fn expand_mismatch_count_errors() {
        let mut config = TunnelConfig {
            tunnel: vec![Tunnel {
                protocol: Protocol::Udp,
                listen: "[::]:7777-7779".into(),
                forward: "127.0.0.1:8888".into(),
                enable: true,
            }],
        };
        assert!(expand_port_ranges(&mut config).is_err());
    }

    #[test]
    fn test_addr_trim_in_load() {
        let dir = std::env::temp_dir().join("ipbridge_test_trim");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"[[tunnel]]
protocol = "udp"
listen = "  0.0.0.0:7777  "
forward = "  127.0.0.1:8888   "
enable = true
"#,
        )
        .unwrap();
        let config = TunnelConfig::load(&path).unwrap();
        assert_eq!(config.tunnel[0].listen, "0.0.0.0:7777");
        assert_eq!(config.tunnel[0].forward, "127.0.0.1:8888");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
