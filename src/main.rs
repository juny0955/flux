use std::{
    error::Error,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    thread::{self},
    time::Duration,
};

use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

fn main() -> Result<(), Box<dyn Error>> {
    setup();

    let listener = TcpListener::bind("127.0.0.1:9000")?;
    info!("9000 port binding completed");

    loop {
        let (client, addr) = listener.accept()?;
        info!("client connected: {}", addr);

        thread::spawn(move || {
            if let Err(e) = handle_client(client) {
                error!("client {} error: {}", addr, e);
            }

            info!("client disconnected: {}", addr);
        });
    }
}

fn handle_client(client: TcpStream) -> io::Result<()> {
    let server =
        TcpStream::connect_timeout(&SocketAddr::from(([127, 0, 0, 1], 8080)), CONNECT_TIMEOUT)?;
    info!("backend connected");

    server.set_read_timeout(Some(READ_TIMEOUT))?;
    server.set_write_timeout(Some(WRITE_TIMEOUT))?;

    client.set_read_timeout(Some(READ_TIMEOUT))?;
    client.set_write_timeout(Some(WRITE_TIMEOUT))?;

    // upstream thread 생성
    let upstream_client = client.try_clone()?;
    let upstream_server = server.try_clone()?;
    let upstream = thread::spawn(move || relay(upstream_client, upstream_server));

    // backend -> client 복사
    let downstream_result = relay(server, client);

    // upstream thread 종료 대기
    let upstream_result = upstream
        .join()
        .map_err(|_| io::Error::other("upstream thread panicked"))?;

    downstream_result?;
    upstream_result?;

    Ok(())
}

fn relay(mut from: TcpStream, mut to: TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 8192];

    loop {
        match handle_recv(&mut from, &mut to, &mut buf) {
            Ok(true) => continue,

            // 정상 EOF
            Ok(false) => {
                let _ = to.shutdown(Shutdown::Write);
                return Ok(());
            }

            // I/O 오류 연결 종료
            Err(e) => {
                let _ = to.shutdown(Shutdown::Both);
                let _ = from.shutdown(Shutdown::Both);
                return Err(e);
            }
        }
    }
}

fn handle_recv(from: &mut TcpStream, to: &mut TcpStream, buf: &mut [u8]) -> io::Result<bool> {
    let n = from.read(buf)?;
    if n == 0 {
        return Ok(false);
    }

    to.write_all(&buf[..n])?;
    Ok(true)
}

fn setup() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}
