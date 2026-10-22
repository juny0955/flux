use std::{
    collections::HashMap,
    error::Error,
    io,
    net::{TcpListener, TcpStream},
    os::fd::{AsRawFd, RawFd},
    ptr,
};

use libc::{kevent, uintptr_t};
use tracing::info;

use crate::{connection::Connection, kqueue::Kqueue};

const MAX_EVENTS: i32 = 64;

pub struct Proxy {
    kq: Kqueue,
    listener: TcpListener,

    connections: HashMap<usize, Connection>,
    fd_to_connection: HashMap<RawFd, usize>,
    next_connection_id: usize,
}

impl Proxy {
    pub fn new(addr: &str) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;

        let kq = Kqueue::new()?;
        kq.register_read(listener.as_raw_fd())?;

        Ok(Self {
            listener,
            kq,
            connections: HashMap::new(),
            fd_to_connection: HashMap::new(),
            next_connection_id: 0,
        })
    }

    pub fn start(&mut self) -> Result<(), Box<dyn Error>> {
        // listener 이벤트 받을 배열 준비
        let mut events = [libc::kevent {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: ptr::null_mut(),
        }; MAX_EVENTS as usize];
        let listener_ident = self.listener.as_raw_fd() as uintptr_t;

        loop {
            let count = self.kq.wait(&mut events)?;
            for event in &events[..count] {
                if event.flags & libc::EV_ERROR != 0 {
                    let code = i32::try_from(event.data)?;
                    return Err(io::Error::from_raw_os_error(code).into());
                }

                if event.ident == listener_ident && event.filter == libc::EVFILT_READ {
                    match self.listener.accept() {
                        Ok((client, _)) => self.new_connection(client)?,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e.into()),
                    }
                } else if event.filter == libc::EVFILT_WRITE {
                    self.handle_write_event(event)?;
                } else if event.filter == libc::EVFILT_READ {
                    self.handle_read_event(event)?;
                }
            }
        }
    }

    fn new_connection(&mut self, client: TcpStream) -> io::Result<()> {
        let connection = Connection::connect(client)?;
        let client_fd = connection.client_fd();
        let server_fd = connection.server_fd();

        if connection.is_connected() {
            // client, server read event 등록
            self.kq.register_read(client_fd)?;
            self.kq.register_read(server_fd)?;
            info!("client 연결: {}", connection.client_addr()?);
        } else {
            // server write event 등록
            self.kq.register_write(server_fd)?;
        }

        self.connections.insert(self.next_connection_id, connection);

        self.fd_to_connection
            .insert(client_fd, self.next_connection_id);
        self.fd_to_connection
            .insert(server_fd, self.next_connection_id);

        self.next_connection_id += 1;
        Ok(())
    }

    fn handle_read_event(&mut self, event: &kevent) -> Result<(), Box<dyn Error>> {
        let ident = i32::try_from(event.ident)?;
        let connection = self.get_connection(ident)?;
        let peer_fd = connection.peer_fd(ident);

        let read_result = connection.read_from(ident)?;
        if read_result == 0 {
            connection.mark_eof(ident);
            self.kq.disable_read(ident)?;
        } else {
            connection.write_to(peer_fd)?;

            if connection.has_pending_data(ident) {
                self.kq.register_write(peer_fd)?;
            }
        }

        Ok(())
    }

    fn handle_write_event(&mut self, event: &kevent) -> Result<(), Box<dyn Error>> {
        let ident = i32::try_from(event.ident)?;
        let connection = self.get_connection(ident)?;

        if connection.is_connected() {
            // 전송 시도 후 데이터가 남아있으면 반대변 소켓 write event 등록
            connection.write_to(ident)?;

            let peer_fd = connection.peer_fd(ident);
            if !connection.has_pending_data(peer_fd) {
                self.kq.disable_write(ident)?;
            }
        } else {
            // 서버 연결시 write 이벤트 해제 후 read 이벤트 등록
            let server_fd = connection.server_fd();
            let client_fd = connection.client_fd();

            connection.finish_connect()?;
            info!("client 연결: {}", connection.client_addr()?);

            self.kq.disable_write(server_fd)?;
            self.kq.register_read(server_fd)?;
            self.kq.register_read(client_fd)?;
        }

        Ok(())
    }

    fn get_connection(&mut self, fd: i32) -> io::Result<&mut Connection> {
        let Some(connection_id) = self.fd_to_connection.get(&fd) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("연결이 등록되지않았습니다. fd: {fd}"),
            ));
        };

        let Some(connection) = self.connections.get_mut(connection_id) else {
            return Err(io::Error::other(format!(
                "fd({fd})에 참조되는 연결이 없습니다. {connection_id}"
            )));
        };

        Ok(connection)
    }
}
