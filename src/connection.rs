use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    os::fd::{AsRawFd, FromRawFd, RawFd},
    ptr,
};

use libc::{AF_INET, IPPROTO_TCP, SOCK_STREAM};

#[derive(Clone, Copy)]
enum ConnectionState {
    Connecting,
    Connected,
}

struct Buffer {
    data: Vec<u8>,
    offset: usize,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offset: 0,
        }
    }

    pub fn read(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub fn extra_buf(&self) -> &[u8] {
        &self.data[self.offset..]
    }

    pub fn write(&mut self, len: usize) {
        self.offset += len;

        if self.is_empty() {
            self.data.clear();
            self.offset = 0;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == self.offset
    }
}

pub struct Connection {
    client: TcpStream,
    server: TcpStream,
    state: ConnectionState,

    upstream_buffer: Buffer,
    downstream_buffer: Buffer,

    client_write_enabled: bool,
    server_write_enabled: bool,
    client_eof: bool,
    server_eof: bool,
}

impl Connection {
    pub fn connect(client: TcpStream) -> io::Result<Self> {
        client.set_nonblocking(true)?;

        let (server, state) = connect_server()?;

        Ok(Self {
            client,
            server,
            state,
            upstream_buffer: Buffer::new(),
            downstream_buffer: Buffer::new(),
            client_write_enabled: false,
            server_write_enabled: false,
            client_eof: false,
            server_eof: false,
        })
    }

    pub fn finish_connect(&mut self) -> io::Result<()> {
        if let Some(error) = self.server.take_error()? {
            return Err(error);
        }

        self.state = ConnectionState::Connected;
        Ok(())
    }

    /// fd: 읽을 대상 소켓
    pub fn read_from(&mut self, fd: i32) -> io::Result<usize> {
        let mut buf = [0_u8; 8192];

        let result = if self.is_client_fd(fd) {
            let n = self.client.read(&mut buf)?;
            self.upstream_buffer.read(&buf[..n]);
            n
        } else {
            let n = self.server.read(&mut buf)?;
            self.downstream_buffer.read(&buf[..n]);
            n
        };

        Ok(result)
    }

    /// fd: 전송할 대상 소켓
    pub fn write_to(&mut self, fd: i32) -> io::Result<()> {
        if self.is_client_fd(fd) {
            let n = self.client.write(self.downstream_buffer.extra_buf())?;
            self.downstream_buffer.write(n);
        } else {
            let n = self.server.write(self.upstream_buffer.extra_buf())?;
            self.upstream_buffer.write(n);
        }

        Ok(())
    }

    pub fn mark_write_enabled(&mut self, fd: i32) {
        if self.is_client_fd(fd) {
            self.client_write_enabled = true;
        } else {
            self.server_write_enabled = true;
        }
    }

    pub fn mark_write_disabled(&mut self, fd: i32) {
        if self.is_client_fd(fd) {
            self.client_write_enabled = false;
        } else {
            self.server_write_enabled = false;
        }
    }

    pub fn mark_eof(&mut self, fd: i32) {
        if self.is_client_fd(fd) {
            self.client_eof = true;
        } else {
            self.server_eof = true;
        }
    }

    pub fn shutdown_write(&mut self, fd: i32) -> io::Result<()> {
        if self.is_client_fd(fd) {
            self.client.shutdown(Shutdown::Write)
        } else {
            self.server.shutdown(Shutdown::Write)
        }
    }

    pub fn peer_fd(&self, fd: i32) -> RawFd {
        if self.is_client_fd(fd) {
            self.server_fd()
        } else {
            self.client_fd()
        }
    }

    pub fn has_pending_data(&self, fd: i32) -> bool {
        if self.is_client_fd(fd) {
            !self.upstream_buffer.is_empty()
        } else {
            !self.downstream_buffer.is_empty()
        }
    }

    pub fn is_write_enabled(&self, fd: i32) -> bool {
        if self.is_client_fd(fd) {
            self.client_write_enabled
        } else {
            self.server_write_enabled
        }
    }

    pub fn is_eof(&self, fd: i32) -> bool {
        if self.is_client_fd(fd) {
            self.client_eof
        } else {
            self.server_eof
        }
    }

    pub fn is_finished(&self) -> bool {
        self.client_eof
            && self.server_eof
            && self.upstream_buffer.is_empty()
            && self.downstream_buffer.is_empty()
    }

    pub fn is_client_fd(&self, fd: i32) -> bool {
        self.client_fd() == fd
    }

    pub fn client_fd(&self) -> RawFd {
        self.client.as_raw_fd()
    }

    pub fn server_fd(&self) -> RawFd {
        self.server.as_raw_fd()
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state, ConnectionState::Connected)
    }

    pub fn client_addr(&self) -> io::Result<SocketAddr> {
        self.client.peer_addr()
    }
}

fn connect_server() -> io::Result<(TcpStream, ConnectionState)> {
    unsafe {
        let server_fd = libc::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if server_fd == -1 {
            return Err(io::Error::last_os_error());
        }

        let server = TcpStream::from_raw_fd(server_fd);
        server.set_nonblocking(true)?;

        let addr_len = std::mem::size_of::<libc::sockaddr_in>();
        let addr = libc::sockaddr_in {
            sin_len: u8::try_from(addr_len).map_err(io::Error::other)?,
            sin_family: libc::sa_family_t::try_from(AF_INET).map_err(io::Error::other)?,
            sin_port: 8080_u16.to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
            },
            sin_zero: [0; 8],
        };

        let result = libc::connect(
            server_fd,
            ptr::from_ref(&addr).cast::<libc::sockaddr>(),
            libc::socklen_t::try_from(addr_len).map_err(io::Error::other)?,
        );

        let state = if result == 0 {
            ConnectionState::Connected
        } else {
            let error = io::Error::last_os_error();

            match error.raw_os_error() {
                Some(libc::EINPROGRESS) => ConnectionState::Connecting,
                _ => return Err(error),
            }
        };

        Ok((server, state))
    }
}
