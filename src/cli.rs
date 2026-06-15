use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// 指定配置文件路径，默认当前目录下 config.toml
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
            .unwrap_or_else(|| PathBuf::from("config.toml"))
    }
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
        assert_eq!(c.config_path(), PathBuf::from("config.toml"));
    }
}
