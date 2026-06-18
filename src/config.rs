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

# 端口范围映射（显式）
# [[tunnel]]
# protocol = "udp"
# listen = "[::]:7777-7780"
# forward = "127.0.0.1:8888-8891"
# enable = true

# 端口范围映射（通配符 * 自动推导数量）
# [[tunnel]]
# protocol = "udp"
# listen = "[::]:7777-7780"
# forward = "127.0.0.1:10000-*"
# enable = true

# 一对一端口镜像（*）
# [[tunnel]]
# protocol = "udp"
# listen = "[::]:7777-7780"
# forward = "127.0.0.1:*"
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum PortBound {
    Value(u16),
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
struct PortRange {
    start: PortBound,
    end: PortBound,
}

impl PortRange {
    fn has_wildcard(&self) -> bool {
        self.start == PortBound::Wildcard || self.end == PortBound::Wildcard
    }

    fn as_explicit(&self) -> (u16, u16) {
        match (self.start, self.end) {
            (PortBound::Value(s), PortBound::Value(e)) => (s, e),
            _ => panic!("as_explicit() called on wildcard range"),
        }
    }
}

/// 解析端口范围字符串为 `PortRange`
///
/// | 输入 | 起始 | 结束 | 含义 |
/// |-------|-------|-----|---------|
/// | `"7777"` | `Value(7777)` | `Value(7777)` | 单端口 |
/// | `"7777-7780"` | `Value(7777)` | `Value(7780)` | 显式范围 |
/// | `"10000-*"` | `Value(10000)` | `Wildcard` | 起始已知，结束 = 起始 + 数量 - 1 |
/// | `"*-10003"` | `Wildcard` | `Value(10003)` | 结束已知，起始 = 结束 - 数量 + 1 |
/// | `"*"` | `Wildcard` | `Wildcard` | 镜像：复制另一侧的端口 1:1 |
fn parse_port_range(s: &str) -> Result<PortRange> {
    if s == "*" {
        return Ok(PortRange {
            start: PortBound::Wildcard,
            end: PortBound::Wildcard,
        });
    }
    if let Some(idx) = s.find('-') {
        let start = match &s[..idx] {
            "*" => PortBound::Wildcard,
            v => PortBound::Value(
                v.parse()
                    .map_err(|_| anyhow::anyhow!("Invalid port: {}", v))?,
            ),
        };
        let end = match &s[idx + 1..] {
            "*" => PortBound::Wildcard,
            v => PortBound::Value(
                v.parse()
                    .map_err(|_| anyhow::anyhow!("Invalid port: {}", v))?,
            ),
        };
        if start == PortBound::Wildcard && end == PortBound::Wildcard {
            anyhow::bail!("Port range '*-*' is ambiguous");
        }
        if let (PortBound::Value(s), PortBound::Value(e)) = (start, end)
            && s > e
        {
            anyhow::bail!("Port range start > end: {}", s);
        }
        Ok(PortRange { start, end })
    } else {
        let port: u16 = s
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid port: {}", s))?;
        Ok(PortRange {
            start: PortBound::Value(port),
            end: PortBound::Value(port),
        })
    }
}

/// 从已知数量的另一侧解析通配符范围
fn resolve_wildcard(range: &PortRange, count: u16) -> Result<(u16, u16)> {
    match (&range.start, &range.end) {
        (PortBound::Value(start), PortBound::Wildcard) => {
            let end = *start as u32 + count as u32 - 1;
            if end > u16::MAX as u32 {
                anyhow::bail!(
                    "Port range {}-* overflows: {} + {} > 65535",
                    start,
                    start,
                    count - 1
                );
            }
            Ok((*start, end as u16))
        }
        (PortBound::Wildcard, PortBound::Value(end)) => {
            if *end < count - 1 {
                anyhow::bail!("Port range *-{} underflows: {} < count {}", end, end, count);
            }
            let start = end - count + 1;
            Ok((start, *end))
        }
        _ => anyhow::bail!("Cannot resolve wildcard range {:?}", range),
    }
}

/// 两侧都是明确范围
fn resolve_ranges(
    lr: &PortRange,
    fr: &PortRange,
    listen_str: &str,
    forward_str: &str,
) -> Result<((u16, u16), (u16, u16))> {
    match (lr.has_wildcard(), fr.has_wildcard()) {
        (false, false) => {
            let (ls, le) = lr.as_explicit();
            let (fs, fe) = fr.as_explicit();
            let lc = le - ls + 1;
            let fc = fe - fs + 1;
            if lc != fc {
                anyhow::bail!(
                    "Port range count mismatch: {} ports in listen, {} in forward",
                    lc,
                    fc
                );
            }
            Ok(((ls, le), (fs, fe)))
        }
        (true, true) => {
            anyhow::bail!(
                "Wildcard '*' on both listen and forward is ambiguous: {} -> {}",
                listen_str,
                forward_str
            );
        }
        (true, false) => {
            let (fs, fe) = fr.as_explicit();
            // Mirror case: listen is bare `*` — copy forward ports 1:1
            if lr.start == PortBound::Wildcard && lr.end == PortBound::Wildcard {
                return Ok(((fs, fe), (fs, fe)));
            }
            let count = fe - fs + 1;
            let (ls, le) = resolve_wildcard(lr, count)?;
            Ok(((ls, le), (fs, fe)))
        }
        (false, true) => {
            let (ls, le) = lr.as_explicit();
            // Mirror case: forward is bare `*` — copy listen ports 1:1
            if fr.start == PortBound::Wildcard && fr.end == PortBound::Wildcard {
                return Ok(((ls, le), (ls, le)));
            }
            let count = le - ls + 1;
            let (fs, fe) = resolve_wildcard(fr, count)?;
            Ok(((ls, le), (fs, fe)))
        }
    }
}

/// 展开端口范围配置成多个单端口配置
/// 同时处理通配符 `*`，根据另一侧的端口数量自动推导范围
fn expand_port_ranges(config: &mut TunnelConfig) -> Result<()> {
    let mut expanded = Vec::new();
    for tunnel in &config.tunnel {
        let (listen_base, listen_port) = split_addr_port(&tunnel.listen)?;
        let (forward_base, forward_port) = split_addr_port(&tunnel.forward)?;

        let lr = parse_port_range(listen_port)?;
        let fr = parse_port_range(forward_port)?;

        // Skip if both are plain single ports (no wildcards, no range)
        if !lr.has_wildcard() && !fr.has_wildcard() && lr.start == lr.end && fr.start == fr.end {
            expanded.push(tunnel.clone());
            continue;
        }

        let ((l_start, l_end), (f_start, _)) =
            resolve_ranges(&lr, &fr, &tunnel.listen, &tunnel.forward)?;

        let count = l_end - l_start + 1;

        for i in 0..count {
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
        let r = parse_port_range("7777").unwrap();
        assert_eq!(r.start, PortBound::Value(7777));
        assert_eq!(r.end, PortBound::Value(7777));
    }

    #[test]
    fn parse_port_range_valid() {
        let r = parse_port_range("7777-7780").unwrap();
        assert_eq!(r.start, PortBound::Value(7777));
        assert_eq!(r.end, PortBound::Value(7780));
    }

    #[test]
    fn parse_port_range_invalid_order() {
        assert!(parse_port_range("7780-7777").is_err());
    }

    #[test]
    fn parse_wildcard_single() {
        let r = parse_port_range("*").unwrap();
        assert_eq!(r.start, PortBound::Wildcard);
        assert_eq!(r.end, PortBound::Wildcard);
    }

    #[test]
    fn parse_wildcard_start() {
        let r = parse_port_range("10000-*").unwrap();
        assert_eq!(r.start, PortBound::Value(10000));
        assert_eq!(r.end, PortBound::Wildcard);
    }

    #[test]
    fn parse_wildcard_end() {
        let r = parse_port_range("*-10003").unwrap();
        assert_eq!(r.start, PortBound::Wildcard);
        assert_eq!(r.end, PortBound::Value(10003));
    }

    #[test]
    fn parse_wildcard_both_sides_errors() {
        assert!(parse_port_range("*-*").is_err());
    }

    #[test]
    fn expand_wildcard_start() {
        let mut config = TunnelConfig {
            tunnel: vec![Tunnel {
                protocol: Protocol::Udp,
                listen: "[::]:7777-7779".into(),
                forward: "127.0.0.1:10000-*".into(),
                enable: true,
            }],
        };
        expand_port_ranges(&mut config).unwrap();
        assert_eq!(config.tunnel.len(), 3);
        assert_eq!(config.tunnel[0].listen, "[::]:7777");
        assert_eq!(config.tunnel[0].forward, "127.0.0.1:10000");
        assert_eq!(config.tunnel[1].listen, "[::]:7778");
        assert_eq!(config.tunnel[1].forward, "127.0.0.1:10001");
        assert_eq!(config.tunnel[2].listen, "[::]:7779");
        assert_eq!(config.tunnel[2].forward, "127.0.0.1:10002");
    }

    #[test]
    fn expand_wildcard_end() {
        let mut config = TunnelConfig {
            tunnel: vec![Tunnel {
                protocol: Protocol::Udp,
                listen: "[::]:7777-7780".into(),
                forward: "127.0.0.1:*-10003".into(),
                enable: true,
            }],
        };
        expand_port_ranges(&mut config).unwrap();
        assert_eq!(config.tunnel.len(), 4);
        assert_eq!(config.tunnel[0].forward, "127.0.0.1:10000");
        assert_eq!(config.tunnel[3].forward, "127.0.0.1:10003");
    }

    #[test]
    fn expand_wildcard_mirror() {
        let mut config = TunnelConfig {
            tunnel: vec![Tunnel {
                protocol: Protocol::Udp,
                listen: "[::]:7777-7779".into(),
                forward: "127.0.0.1:*".into(),
                enable: true,
            }],
        };
        expand_port_ranges(&mut config).unwrap();
        assert_eq!(config.tunnel.len(), 3);
        // forward port mirrors listen port 1:1
        assert_eq!(config.tunnel[0].forward, "127.0.0.1:7777");
        assert_eq!(config.tunnel[2].forward, "127.0.0.1:7779");
    }

    #[test]
    fn expand_wildcard_reverse_listen() {
        let mut config = TunnelConfig {
            tunnel: vec![Tunnel {
                protocol: Protocol::Udp,
                listen: "[::]:*-7780".into(),
                forward: "127.0.0.1:10000-10003".into(),
                enable: true,
            }],
        };
        expand_port_ranges(&mut config).unwrap();
        assert_eq!(config.tunnel.len(), 4);
        assert_eq!(config.tunnel[0].listen, "[::]:7777");
        assert_eq!(config.tunnel[3].listen, "[::]:7780");
    }

    #[test]
    fn expand_wildcard_mirror_reverse() {
        let mut config = TunnelConfig {
            tunnel: vec![Tunnel {
                protocol: Protocol::Udp,
                listen: "[::]:*".into(),
                forward: "127.0.0.1:10000-10002".into(),
                enable: true,
            }],
        };
        expand_port_ranges(&mut config).unwrap();
        assert_eq!(config.tunnel.len(), 3);
        // listen mirrors forward ports 1:1
        assert_eq!(config.tunnel[0].listen, "[::]:10000");
        assert_eq!(config.tunnel[2].listen, "[::]:10002");
    }

    #[test]
    fn expand_wildcard_both_sides_errors() {
        let mut config = TunnelConfig {
            tunnel: vec![Tunnel {
                protocol: Protocol::Udp,
                listen: "[::]:*".into(),
                forward: "127.0.0.1:*".into(),
                enable: true,
            }],
        };
        assert!(expand_port_ranges(&mut config).is_err());
    }

    #[test]
    fn expand_wildcard_one_side_single_port_errors() {
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
