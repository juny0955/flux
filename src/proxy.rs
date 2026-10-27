use std::{
    collections::HashMap,
    error::Error,
    io::{self, ErrorKind},
    net::{TcpListener, TcpStream},
    os::fd::{AsRawFd, RawFd},
    ptr,
};

use libc::kevent;
use tracing::warn;

use crate::{
    config::Config,
    connection::{Connection, ReadResult},
    kqueue::Kqueue,
};

const MAX_EVENTS: i32 = 64;

struct ConnectionStore {
    connections: HashMap<usize, Connection>,
    fd_to_connection: HashMap<RawFd, usize>,
    next_connection_id: usize,
}

impl ConnectionStore {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            fd_to_connection: HashMap::new(),
            next_connection_id: 0,
        }
    }

    pub fn insert(&mut self, connection: Connection) {
        let client_fd = connection.client_fd();
        let server_fd = connection.server_fd();
        self.connections.insert(self.next_connection_id, connection);

        self.fd_to_connection
            .insert(client_fd, self.next_connection_id);
        self.fd_to_connection
            .insert(server_fd, self.next_connection_id);

        self.next_connection_id += 1;
    }

    /// 대상 소켓과 연결된 connection을 제거한다
    pub fn remove_connection(&mut self, fd: RawFd) {
        if let Some(id) = self.fd_to_connection.remove(&fd)
            && let Some(connection) = self.connections.remove(&id)
        {
            self.fd_to_connection.remove(&connection.client_fd());
            self.fd_to_connection.remove(&connection.server_fd());
        }
    }

    pub fn connection_mut(&mut self, fd: RawFd) -> Option<&mut Connection> {
        let connection_id = self.fd_to_connection.get(&fd)?;
        self.connections.get_mut(connection_id)
    }
}

pub struct Proxy {
    config: Config,
    kq: Kqueue,
    listener: TcpListener,
    connections: ConnectionStore,
}

impl Proxy {
    pub fn new(config: Config) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind(config.listen)?;
        listener.set_nonblocking(true)?;

        let kq = Kqueue::new()?;
        kq.enable_read(listener.as_raw_fd())?;

        Ok(Self {
            config,
            listener,
            kq,
            connections: ConnectionStore::new(),
        })
    }

    pub fn start(&mut self) -> io::Result<()> {
        // listener 이벤트 받을 배열 준비
        let mut events = [libc::kevent {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: ptr::null_mut(),
        }; MAX_EVENTS as usize];

        loop {
            let count = match self.kq.wait(&mut events) {
                Ok(count) => count,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => {
                    warn!(%e, "kqueue 이벤트 수신 중 오류");
                    return Err(e);
                }
            };

            for event in &events[..count] {
                if let Err(e) = self.dispatch_event(event) {
                    let fd = event.ident as RawFd;
                    warn!(%e, fd, "event 처리 실패");

                    if fd != self.listener.as_raw_fd() {
                        self.connections.remove_connection(fd);
                    }
                }
            }
        }
    }

    fn dispatch_event(&mut self, event: &kevent) -> io::Result<()> {
        if event.flags & libc::EV_ERROR != 0 {
            let error = io::Error::from_raw_os_error(event.data as i32);
            return Err(error);
        }

        if event.ident as RawFd == self.listener.as_raw_fd() && event.filter == libc::EVFILT_READ {
            return self.handle_accept(event);
        }

        match event.filter {
            libc::EVFILT_READ => self.handle_read_event(event),
            libc::EVFILT_WRITE => self.handle_write_event(event),
            _ => Ok(()),
        }
    }

    fn handle_accept(&mut self, event: &kevent) -> io::Result<()> {
        let avaliable = event.data.max(0) as usize;
        let mut accepted = 0;

        while accepted < avaliable {
            match self.listener.accept() {
                Ok((client, addr)) => {
                    accepted += 1;

                    if let Err(e) = self.new_connection(client) {
                        warn!(%e, %addr, "신규 연결 생성 실패");
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    fn handle_read_event(&mut self, event: &kevent) -> io::Result<()> {
        let fd = event.ident as RawFd;
        let Some(connection) = self.connections.connection_mut(fd) else {
            warn!(fd, "connection 조회 실패");
            return Ok(());
        };

        let peer_fd = connection.peer_fd(fd);
        let result = match connection.read(fd) {
            Ok(result) => result,
            Err(e) => {
                warn!(fd, peer_fd, %e, "connection 읽기 실패");
                self.connections.remove_connection(fd);
                return Ok(());
            }
        };

        match result {
            ReadResult::Data => {
                if let Err(e) = connection.flush(peer_fd) {
                    warn!(fd = peer_fd, peer_fd = fd, %e, "connection 쓰기 실패");
                    self.connections.remove_connection(fd);
                    return Ok(());
                }

                update_write_interest(connection, &self.kq, fd)?;

                if connection.is_buffer_full(fd) {
                    connection.disable_read(&self.kq, fd)?;
                } else {
                    connection.enable_read(&self.kq, fd)?;
                }
            }
            ReadResult::BufferFull => {
                if connection.has_pending_data(fd)
                    && let Err(e) = connection.flush(peer_fd)
                {
                    warn!(fd = peer_fd, peer_fd = fd, %e, "connection 쓰기 실패");
                    self.connections.remove_connection(fd);
                    return Ok(());
                }

                if !connection.is_buffer_full(fd) && !connection.is_eof(fd) {
                    connection.enable_read(&self.kq, fd)?;
                } else {
                    connection.disable_read(&self.kq, fd)?;
                }

                update_write_interest(connection, &self.kq, fd)?;
            }
            ReadResult::Eof => {
                connection.mark_eof(&self.kq, fd)?;

                if let Err(e) = connection.flush(peer_fd) {
                    warn!(fd, peer_fd, %e, "connection 쓰기 실패");
                    self.connections.remove_connection(fd);
                    return Ok(());
                }

                update_write_interest(connection, &self.kq, fd)?;

                if !connection.has_pending_data(fd) {
                    connection.shutdown_write(peer_fd)?;
                }

                if connection.is_finished() {
                    self.connections.remove_connection(fd);
                }

                return Ok(());
            }
        }

        Ok(())
    }

    fn handle_write_event(&mut self, event: &kevent) -> io::Result<()> {
        let fd = event.ident as RawFd;
        let connection = match self.connections.connection_mut(fd) {
            Some(connection) => connection,
            None => {
                warn!(fd, "connection 조회 실패");
                return Ok(());
            }
        };

        if connection.is_connected() {
            let peer_fd = connection.peer_fd(fd);
            // 전송 시도 후 데이터가 남아있으면 반대변 소켓 write event 등록
            if connection.has_pending_data(peer_fd)
                && let Err(e) = connection.flush(fd)
            {
                warn!(fd, peer_fd, %e, "connection 쓰기 실패");
                self.connections.remove_connection(fd);
                return Ok(());
            }

            if !connection.is_buffer_full(peer_fd) && !connection.is_eof(peer_fd) {
                connection.enable_read(&self.kq, peer_fd)?;
            }

            update_write_interest(connection, &self.kq, peer_fd)?;

            if connection.is_eof(peer_fd) && !connection.has_pending_data(peer_fd) {
                connection.shutdown_write(fd)?;
            }

            if connection.is_finished() {
                self.connections.remove_connection(fd);
            }
        } else {
            // 서버 연결시 write 이벤트 해제 후 read 이벤트 등록
            let server_fd = connection.server_fd();
            if let Err(e) = connection.finish_connect() {
                warn!(%e, "서버 연결 실패");
                self.connections.remove_connection(server_fd);
                return Ok(());
            }
            // info!("client 연결 {}", connection.client_addr()?);

            connection.disable_write(&self.kq, server_fd)?;
            register_connected(connection, &self.kq)?;
        }

        Ok(())
    }

    fn new_connection(&mut self, client: TcpStream) -> io::Result<()> {
        let mut connection =
            Connection::connect(client, self.config.upstream, self.config.buffer_size.get())?;

        if connection.is_connected() {
            register_connected(&mut connection, &self.kq)?;
            // info!("client 연결: {}", connection.client_addr()?);
        } else {
            // server write event 등록
            connection.enable_write(&self.kq, connection.server_fd())?;
        }

        self.connections.insert(connection);

        Ok(())
    }
}

fn update_write_interest(
    connection: &mut Connection,
    kq: &Kqueue,
    source_fd: RawFd,
) -> io::Result<()> {
    let target_fd = connection.peer_fd(source_fd);

    if connection.has_pending_data(source_fd) {
        connection.enable_write(kq, target_fd)
    } else {
        connection.disable_write(kq, target_fd)
    }
}

// client, server read event 등록
fn register_connected(connection: &mut Connection, kq: &Kqueue) -> io::Result<()> {
    connection.enable_read(kq, connection.client_fd())?;
    connection.enable_read(kq, connection.server_fd())?;
    Ok(())
}
