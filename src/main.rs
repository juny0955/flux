use std::error::Error;

use clap::Parser;
use flux::{config::Config, proxy::Proxy};
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse();
    setup();

    let listen = config.listen;
    let mut proxy = Proxy::new(config)?;
    info!(%listen, "listener binding completed");
    proxy.start()?;

    Ok(())
}

fn setup() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
