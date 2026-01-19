use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// 指定配置文件路径，默认与可执行文件同目录下 config.toml
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,
}

impl Cli {
    pub fn config_path(&self) -> PathBuf {
        if let Some(ref path) = self.config {
            path.clone()
        } else {
            // 默认可执行文件同目录下 config.toml
            std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(|p| p.join("config.toml")))
                .unwrap_or_else(|| PathBuf::from("config.toml"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    #[test]
    fn config_path_with_option() {
        let c = Cli {
            config: Some(PathBuf::from("foo.toml")),
        };
        assert_eq!(c.config_path(), PathBuf::from("foo.toml"));
    }

    #[test]
    fn config_path_default_has_config_toml() {
        let c = Cli { config: None };
        assert_eq!(c.config_path().file_name(), Some(OsStr::new("config.toml")));
    }
}
