use std::error::Error;

use clap::Parser;
use flux::{
    config::{Cli, Config},
    proxy::Proxy,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let config = match Config::load(cli) {
        Ok(config) => config,
        Err(error) => {
            clap::Error::raw(clap::error::ErrorKind::ValueValidation, error.to_string()).exit()
        }
    };
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
