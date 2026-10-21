use std::{
    io::{self},
    net::{SocketAddr, TcpStream},
    thread,
    time::Duration,
};

mod connection;

use tracing::info;

use crate::proxy::connection::Connection;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub fn handle_client(client: TcpStream) -> io::Result<()> {
    let server =
        TcpStream::connect_timeout(&SocketAddr::from(([127, 0, 0, 1], 8080)), CONNECT_TIMEOUT)?;
    info!("backend connected");

    // upstream thread 생성
    let upstream = Connection::new(client.try_clone()?, server.try_clone()?)?;
    let upstream = thread::spawn(move || relay(upstream));

    // backend -> client 복사
    let downstream_result = relay(Connection::new(server, client)?);

    // upstream thread 종료 대기
    let upstream_result = upstream
        .join()
        .map_err(|_| io::Error::other("upstream thread panicked"))?;

    downstream_result?;
    upstream_result?;

    Ok(())
}

fn relay(mut connection: Connection) -> io::Result<()> {
    loop {
        if connection.can_read() {
            connection.read()?;
        }

        connection.write()?;
    }
}
