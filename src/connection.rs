use std::{
    io::{self, Read, Write},
    net::{Shutdown, TcpStream},
    os::fd::{AsRawFd, FromRawFd, RawFd},
    ptr,
};

use libc::{AF_INET, IPPROTO_TCP, SOCK_STREAM};

const BUFFER_SIZE: usize = 16 * 1024; // 16KB

#[derive(Clone, Copy)]
enum ConnectionState {
    Connecting,
    Connected,
}

struct Buffer {
    data: [u8; BUFFER_SIZE],
    read_pos: usize,
    write_pos: usize,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            data: [0u8; BUFFER_SIZE],
            read_pos: 0,
            write_pos: 0,
        }
    }

    fn compact(&mut self) {
        if self.read_pos == 0 {
            return;
        }

        self.data.copy_within(self.read_pos..self.write_pos, 0);

        self.write_pos -= self.read_pos;
        self.read_pos = 0;
    }

    pub fn is_full(&self) -> bool {
        self.write_pos - self.read_pos == BUFFER_SIZE
    }

    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }
}

pub enum ReadResult {
    Data,
    Eof,
    BufferFull,
}

pub struct Connection {
    client: TcpStream,
    server: TcpStream,
    state: ConnectionState,

    upstream_buffer: Buffer,
    downstream_buffer: Buffer,

    clinet_read_enabled: bool,
    server_read_enabled: bool,
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
            clinet_read_enabled: false,
            server_read_enabled: false,
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
    pub fn read_from(&mut self, fd: RawFd) -> io::Result<ReadResult> {
        let (stream, buffer) = if self.is_client_fd(fd) {
            (&mut self.client, &mut self.upstream_buffer)
        } else {
            (&mut self.server, &mut self.downstream_buffer)
        };

        if buffer.write_pos == BUFFER_SIZE {
            buffer.compact();
        }
        if buffer.write_pos == BUFFER_SIZE {
            return Ok(ReadResult::BufferFull);
        }

        let n = stream.read(&mut buffer.data[buffer.write_pos..])?;

        if n == 0 {
            return Ok(ReadResult::Eof);
        }

        buffer.write_pos += n;
        Ok(ReadResult::Data)
    }

    /// fd: 전송할 대상 소켓
    pub fn write_to(&mut self, fd: RawFd) -> io::Result<()> {
        let (stream, buffer) = if self.is_client_fd(fd) {
            (&mut self.client, &mut self.downstream_buffer)
        } else {
            (&mut self.server, &mut self.upstream_buffer)
        };

        if buffer.is_empty() {
            return Ok(());
        }

        let n = stream.write(&buffer.data[buffer.read_pos..buffer.write_pos])?;
        buffer.read_pos += n;

        if n == 0 {
            buffer.read_pos = 0;
            buffer.write_pos = 0;
        }

        Ok(())
    }

    pub fn mark_read_enabled(&mut self, fd: RawFd) {
        if self.is_client_fd(fd) {
            self.clinet_read_enabled = true;
        } else {
            self.server_read_enabled = true;
        }
    }

    pub fn mark_read_disabled(&mut self, fd: RawFd) {
        if self.is_client_fd(fd) {
            self.clinet_read_enabled = false;
        } else {
            self.server_read_enabled = false;
        }
    }

    pub fn mark_write_enabled(&mut self, fd: RawFd) {
        if self.is_client_fd(fd) {
            self.client_write_enabled = true;
        } else {
            self.server_write_enabled = true;
        }
    }

    pub fn mark_write_disabled(&mut self, fd: RawFd) {
        if self.is_client_fd(fd) {
            self.client_write_enabled = false;
        } else {
            self.server_write_enabled = false;
        }
    }

    pub fn mark_eof(&mut self, fd: RawFd) {
        if self.is_client_fd(fd) {
            self.client_eof = true;
        } else {
            self.server_eof = true;
        }
    }

    pub fn shutdown_write(&mut self, fd: RawFd) -> io::Result<()> {
        if self.is_client_fd(fd) {
            self.client.shutdown(Shutdown::Write)
        } else {
            self.server.shutdown(Shutdown::Write)
        }
    }

    pub fn peer_fd(&self, fd: RawFd) -> RawFd {
        if self.is_client_fd(fd) {
            self.server_fd()
        } else {
            self.client_fd()
        }
    }

    pub fn has_pending_data(&self, fd: RawFd) -> bool {
        if self.is_client_fd(fd) {
            !self.upstream_buffer.is_empty()
        } else {
            !self.downstream_buffer.is_empty()
        }
    }

    pub fn is_buffer_full(&self, fd: RawFd) -> bool {
        if self.is_client_fd(fd) {
            self.upstream_buffer.is_full()
        } else {
            self.downstream_buffer.is_full()
        }
    }

    pub fn is_read_enabled(&self, fd: RawFd) -> bool {
        if self.is_client_fd(fd) {
            self.clinet_read_enabled
        } else {
            self.server_read_enabled
        }
    }

    pub fn is_write_enabled(&self, fd: RawFd) -> bool {
        if self.is_client_fd(fd) {
            self.client_write_enabled
        } else {
            self.server_write_enabled
        }
    }

    pub fn is_eof(&self, fd: RawFd) -> bool {
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
}

fn connect_server() -> io::Result<(TcpStream, ConnectionState)> {
    unsafe {
        // server 소켓 생성
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
