use std::error::Error;

use flux::proxy::Proxy;
use tracing::info;
use tracing_subscriber::EnvFilter;

const ADDR: &str = "127.0.0.1:9000";

fn main() -> Result<(), Box<dyn Error>> {
    setup();

    let mut proxy = Proxy::new(ADDR)?;
    info!("listener binding completed: {ADDR}");
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
