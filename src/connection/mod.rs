use std::{
    io::{self, Error, ErrorKind, Read, Write},
    net::{SocketAddrV4, TcpStream},
    os::fd::{FromRawFd, RawFd},
    ptr,
};

use libc::{AF_INET, IPPROTO_TCP, SOCK_STREAM};

use crate::{
    connection::{buffer::Buffer, endpoint::Endpoint},
    kqueue::Kqueue,
};

mod buffer;
mod endpoint;

#[derive(Clone, Copy)]
enum ConnectionState {
    Connecting,
    Connected,
}

pub enum ReadResult {
    Data(usize),
    Eof,
    BufferFull,
}

pub struct Connection {
    client: Endpoint,
    server: Endpoint,
    state: ConnectionState,

    upstream_buffer: Buffer,
    downstream_buffer: Buffer,
}

impl Connection {
    pub fn connect(
        client: TcpStream,
        upstream: SocketAddrV4,
        buffer_size: usize,
    ) -> io::Result<Self> {
        client.set_nonblocking(true)?;

        let (server, state) = connect_server(upstream)?;

        Ok(Self {
            client: Endpoint::new(client),
            server: Endpoint::new(server),
            state,
            upstream_buffer: Buffer::new(buffer_size),
            downstream_buffer: Buffer::new(buffer_size),
        })
    }

    /// 조건에 도달할때까지 read를 시도한다
    /// 1. buffer가 가득 한 경우
    /// 2. event.data의 바이트 수만큼 읽은 경우
    /// 3. WouldBlock인 경우
    pub fn read(&mut self, fd: RawFd, byte_len: usize) -> io::Result<ReadResult> {
        let mut read_bytes = 0;
        loop {
            if self.is_buffer_full(fd) {
                return Ok(ReadResult::BufferFull);
            }

            match self.read_from(fd) {
                Ok(ReadResult::Data(n)) => {
                    read_bytes += n;
                    if read_bytes >= byte_len {
                        return Ok(ReadResult::Data(read_bytes));
                    }

                    continue;
                }
                Ok(result) => return Ok(result),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    return Ok(ReadResult::Data(read_bytes));
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// fd: 읽을 대상 소켓
    pub fn read_from(&mut self, fd: RawFd) -> io::Result<ReadResult> {
        let (endpoint, buffer) = if self.is_client_fd(fd) {
            (&mut self.client, &mut self.upstream_buffer)
        } else {
            (&mut self.server, &mut self.downstream_buffer)
        };

        if buffer.write_pos == buffer.buffer_size() {
            buffer.compact();
        }
        if buffer.write_pos == buffer.buffer_size() {
            return Ok(ReadResult::BufferFull);
        }

        let n = endpoint.stream.read(&mut buffer.data[buffer.write_pos..])?;
        if n == 0 {
            return Ok(ReadResult::Eof);
        }

        buffer.write_pos += n;
        Ok(ReadResult::Data(n))
    }

    /// 해당 fd 버퍼를 전부 비우거나 WouldBlock 될 때까지 write 시도
    pub fn flush(&mut self, fd: RawFd) -> io::Result<()> {
        loop {
            match self.write_to(fd) {
                Ok(()) => {
                    if !self.has_pending_data(self.peer_fd(fd)) {
                        return Ok(());
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// fd: 전송할 대상 소켓
    fn write_to(&mut self, fd: RawFd) -> io::Result<()> {
        let (endpoint, buffer) = if self.is_client_fd(fd) {
            (&mut self.client, &mut self.downstream_buffer)
        } else {
            (&mut self.server, &mut self.upstream_buffer)
        };

        if buffer.is_empty() {
            return Ok(());
        }

        let n = endpoint
            .stream
            .write(&buffer.data[buffer.read_pos..buffer.write_pos])?;

        if n == 0 {
            return Err(Error::new(ErrorKind::WriteZero, "write zero"));
        }

        buffer.read_pos += n;

        if buffer.is_empty() {
            buffer.read_pos = 0;
            buffer.write_pos = 0;
        }

        Ok(())
    }

    pub fn finish_connect(&mut self) -> io::Result<()> {
        if let Some(error) = self.server.take_error()? {
            return Err(error);
        }

        self.state = ConnectionState::Connected;
        Ok(())
    }

    pub fn enable_read(&mut self, kq: &mut Kqueue, fd: RawFd) {
        self.endpoint_mut(fd).enable_read(kq)
    }

    pub fn disable_read(&mut self, kq: &mut Kqueue, fd: RawFd) {
        self.endpoint_mut(fd).disable_read(kq)
    }

    pub fn enable_write(&mut self, kq: &mut Kqueue, fd: RawFd) {
        self.endpoint_mut(fd).enable_write(kq)
    }

    pub fn disable_write(&mut self, kq: &mut Kqueue, fd: RawFd) {
        self.endpoint_mut(fd).disable_write(kq)
    }

    pub fn shutdown_write(&mut self, fd: RawFd) -> io::Result<()> {
        self.endpoint_mut(fd).shutdown_write()
    }

    /// READ Event 비활성화 후 eof 마킹
    pub fn mark_eof(&mut self, kq: &mut Kqueue, fd: RawFd) {
        let endpoint = self.endpoint_mut(fd);
        endpoint.disable_read(kq);
        endpoint.read_eof = true;
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

    pub fn is_eof(&self, fd: RawFd) -> bool {
        self.endpoint(fd).read_eof
    }

    pub fn is_finished(&self) -> bool {
        self.client.read_eof
            && self.server.read_eof
            && self.upstream_buffer.is_empty()
            && self.downstream_buffer.is_empty()
    }

    pub fn client_fd(&self) -> RawFd {
        self.client.raw_fd()
    }

    pub fn server_fd(&self) -> RawFd {
        self.server.raw_fd()
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state, ConnectionState::Connected)
    }

    fn endpoint(&self, fd: RawFd) -> &Endpoint {
        if self.is_client_fd(fd) {
            &self.client
        } else {
            &self.server
        }
    }

    fn endpoint_mut(&mut self, fd: RawFd) -> &mut Endpoint {
        if self.is_client_fd(fd) {
            &mut self.client
        } else {
            &mut self.server
        }
    }

    fn is_client_fd(&self, fd: RawFd) -> bool {
        self.client_fd() == fd
    }
}

fn connect_server(upstream: SocketAddrV4) -> io::Result<(TcpStream, ConnectionState)> {
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
            sin_port: upstream.port().to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(upstream.ip().octets()),
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
