use clap::{Parser, Subcommand};
use std::path::PathBuf;

pub const DEFAULT_CONFIG_NAME: &str = "config.toml";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// 指定配置文件路径。默认依次查找当前目录、$XDG_CONFIG_HOME/ipbridge、~/.config/ipbridge
    #[arg(short, long, value_name = "FILE", global = true)]
    pub config: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// 生成配置模板
    Init {
        /// 覆盖已有配置文件
        #[arg(short, long)]
        force: bool,
    },
    /// 检查隧道配置是否可用
    Check,
    /// 运行代理（默认行为，可省略）
    Run,
}

impl Cli {
    pub fn config_path(&self) -> PathBuf {
        self.config
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_NAME))
    }

    pub fn resolve_config_path(&self) -> Option<PathBuf> {
        if let Some(p) = &self.config {
            return Some(p.clone());
        }
        let candidates = std::iter::once(PathBuf::from(DEFAULT_CONFIG_NAME))
            .chain(xdg_config_dir().map(|d| d.join("ipbridge").join(DEFAULT_CONFIG_NAME)));
        candidates.into_iter().find(|p| p.exists())
    }
}

fn xdg_config_dir() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("XDG_CONFIG_HOME")
        && !dir.is_empty()
    {
        return Some(PathBuf::from(dir));
    }
    std::env::var("HOME")
        .ok()
        .filter(|h| !h.is_empty())
        .map(|h| PathBuf::from(h).join(".config"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_path_with_option() {
        let c = Cli {
            command: None,
            config: Some(PathBuf::from("foo.toml")),
        };
        assert_eq!(c.config_path(), PathBuf::from("foo.toml"));
    }

    #[test]
    fn config_path_default() {
        let c = Cli {
            command: None,
            config: None,
        };
        assert_eq!(c.config_path(), PathBuf::from(DEFAULT_CONFIG_NAME));
    }

    #[test]
    fn resolve_config_path_with_option() {
        let c = Cli {
            command: None,
            config: Some(PathBuf::from("foo.toml")),
        };
        assert_eq!(
            c.resolve_config_path(),
            Some(PathBuf::from("foo.toml")),
            "explicit --config must win even if the file is missing"
        );
    }

    #[test]
    fn resolve_config_path_search_order() {
        let base = std::env::temp_dir().join("ipbridge_test_resolve");
        let _ = std::fs::remove_dir_all(&base);
        let empty = base.join("empty");
        let xdg = base.join("xdg");
        let home = base.join("home");
        std::fs::create_dir_all(&empty).unwrap();
        std::fs::create_dir_all(xdg.join("ipbridge")).unwrap();
        std::fs::create_dir_all(home.join(".config/ipbridge")).unwrap();
        std::fs::write(xdg.join("ipbridge/config.toml"), "").unwrap();
        std::fs::write(home.join(".config/ipbridge/config.toml"), "").unwrap();

        let c = Cli {
            command: None,
            config: None,
        };

        std::env::set_current_dir(&empty).unwrap();
        unsafe { std::env::set_var("XDG_CONFIG_HOME", &xdg) };
        assert_eq!(
            c.resolve_config_path(),
            Some(xdg.join("ipbridge/config.toml")),
            "should fall back to XDG_CONFIG_HOME when CWD has no config"
        );

        std::fs::write(empty.join(DEFAULT_CONFIG_NAME), "").unwrap();
        assert_eq!(
            c.resolve_config_path(),
            Some(PathBuf::from(DEFAULT_CONFIG_NAME)),
            "CWD config must win over XDG"
        );
        std::fs::remove_file(empty.join(DEFAULT_CONFIG_NAME)).unwrap();

        unsafe { std::env::remove_var("XDG_CONFIG_HOME") };
        unsafe { std::env::set_var("HOME", &home) };
        assert_eq!(
            c.resolve_config_path(),
            Some(home.join(".config/ipbridge/config.toml")),
            "should fall back to HOME/.config when XDG_CONFIG_HOME is unset"
        );

        unsafe { std::env::remove_var("HOME") };
        assert_eq!(
            c.resolve_config_path(),
            None,
            "should be None when no config exists anywhere"
        );

        std::env::set_current_dir(env!("CARGO_MANIFEST_DIR")).unwrap();
        let _ = std::fs::remove_dir_all(&base);
    }
}
