use std::{
    error::Error,
    io::{Read, Write, stdin},
    net::TcpStream,
};

use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error>> {
    setup();
    let mut stream = TcpStream::connect("127.0.0.1:9000")?;
    info!("127.0.0.1:9000 connected");

    let mut input = String::new();
    let mut buf = [0u8; 1024];
    loop {
        stdin().read_line(&mut input)?;
        stream.write_all(input.as_bytes())?;
        input.clear();

        let n = stream.read(&mut buf)?;
        info!("read: {}", String::from_utf8_lossy(&buf[..n]));
    }
}

fn setup() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
