use std::{
    error::Error,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    thread,
};

use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error>> {
    setup();

    let listener = TcpListener::bind("127.0.0.1:8080")?;
    info!("8080 port binding completed");

    loop {
        let (client, addr) = listener.accept()?;
        info!("client connected: {}", addr);

        thread::spawn(move || {
            let _ = handle_connection(client, addr);
        });
    }
}

fn handle_connection(mut client: TcpStream, addr: SocketAddr) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        let n = client.read(&mut buf)?;

        if n == 0 {
            info!("client disconnected: {}", addr);
            break;
        }

        info!("read: {}", String::from_utf8_lossy(&buf[..n]).trim());
        client.write_all(&buf[..n])?;
    }

    Ok(())
}

fn setup() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
