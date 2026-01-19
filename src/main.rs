mod cli;
mod config;
mod proxy;

use clap::Parser;
use cli::Cli;
use config::TunnelConfig;
use proxy::run_proxy;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let cli = Cli::parse();
    let config_path = cli.config_path();
    let config = TunnelConfig::load_or_create(config_path.to_str().unwrap());
    let proxy_task = tokio::spawn(run_proxy(config));

    // Ctrl+C 双击退出
    tokio::spawn(async {
        let mut first = true;
        loop {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl_c");
            if first {
                eprintln!("再按一次Ctrl+C退出");
                first = false;
            } else {
                std::process::exit(0);
            }
        }
    });

    proxy_task.await.expect("Proxy task panicked");
}
