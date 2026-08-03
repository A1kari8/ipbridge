mod cli;
mod config;
mod tunnel;

use clap::Parser;
use cli::{Cli, Command};
use config::TunnelConfig;
use tunnel::run;

fn exit(msg: &str) -> ! {
    eprintln!("Error: {}", msg);
    std::process::exit(1);
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let config_path = cli.config_path();

    match &cli.command {
        Some(Command::Check) => {}
        _ => {
            env_logger::Builder::from_default_env()
                .format_target(false)
                .filter_level(log::LevelFilter::Info)
                .init();
        }
    }

    match cli.command {
        Some(Command::Init { force }) => match TunnelConfig::create_template(&config_path, force) {
            Ok(()) => {
                eprintln!("Config template generated at {}", config_path.display());
            }
            Err(e) => exit(&e.to_string()),
        },
        Some(Command::Check) => {
            let config = match cli.resolve_config_path() {
                Some(path) => TunnelConfig::load(&path).unwrap_or_else(|e| exit(&e.to_string())),
                None => exit("No config file found. Run `ipbridge init` to generate a template."),
            };
            for tunnel in &config.tunnel {
                tunnel::check(tunnel).await;
                println!();
            }
        }
        Some(Command::Run) | None => {
            let config = match cli.resolve_config_path() {
                Some(path) => TunnelConfig::load(&path).unwrap_or_else(|e| exit(&e.to_string())),
                None => exit("No config file found. Run `ipbridge init` to generate a template."),
            };

            tokio::spawn(async {
                tokio::signal::ctrl_c().await.unwrap();
                std::process::exit(0);
            });

            run(config).await;
        }
    }
}
