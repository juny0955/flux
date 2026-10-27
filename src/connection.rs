use std::{
    io::{self, Error, ErrorKind, Read, Write},
    net::{Shutdown, SocketAddrV4, TcpStream},
    os::fd::{AsRawFd, FromRawFd, RawFd},
    ptr,
};

use libc::{AF_INET, IPPROTO_TCP, SOCK_STREAM};

use crate::kqueue::Kqueue;

#[derive(Clone, Copy)]
enum ConnectionState {
    Connecting,
    Connected,
}

struct Buffer {
    data: Vec<u8>,
    read_pos: usize,
    write_pos: usize,
}

impl Buffer {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            data: vec![0; buffer_size],
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
        self.write_pos - self.read_pos == self.buffer_size()
    }

    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }

    pub fn buffer_size(&self) -> usize {
        self.data.len()
    }
}

pub enum ReadResult {
    Data,
    Eof,
    BufferFull,
}

struct Endpoint {
    stream: TcpStream,
    read_enabled: bool,
    write_enabled: bool,
    read_eof: bool,
}

impl Endpoint {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_enabled: false,
            write_enabled: false,
            read_eof: false,
        }
    }

    pub fn enable_read(&mut self, kq: &Kqueue) -> io::Result<()> {
        if !self.read_enabled {
            kq.enable_read(self.raw_fd())?;
            self.read_enabled = true;
        }

        Ok(())
    }

    pub fn enable_write(&mut self, kq: &Kqueue) -> io::Result<()> {
        if !self.write_enabled {
            kq.enable_write(self.raw_fd())?;
            self.write_enabled = true;
        }

        Ok(())
    }

    pub fn disable_read(&mut self, kq: &Kqueue) -> io::Result<()> {
        if self.read_enabled {
            kq.disable_read(self.raw_fd())?;
            self.read_enabled = false;
        }

        Ok(())
    }

    pub fn disable_write(&mut self, kq: &Kqueue) -> io::Result<()> {
        if self.write_enabled {
            kq.disable_write(self.raw_fd())?;
            self.write_enabled = false;
        }

        Ok(())
    }

    pub fn shutdown_write(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Write)
    }

    pub fn raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.stream.take_error()
    }
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

    /// 해당 fd 버퍼가 다 차거나 WouldBlock 될 때 까지 read 시도
    pub fn read(&mut self, fd: RawFd) -> io::Result<ReadResult> {
        loop {
            if self.is_buffer_full(fd) {
                return Ok(ReadResult::BufferFull);
            }

            match self.read_from(fd) {
                Ok(ReadResult::Data) => continue,
                Ok(result) => return Ok(result),
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(ReadResult::Data),
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
        Ok(ReadResult::Data)
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

    pub fn enable_read(&mut self, kq: &Kqueue, fd: RawFd) -> io::Result<()> {
        self.endpoint_mut(fd).enable_read(kq)
    }

    pub fn disable_read(&mut self, kq: &Kqueue, fd: RawFd) -> io::Result<()> {
        self.endpoint_mut(fd).disable_read(kq)
    }

    pub fn enable_write(&mut self, kq: &Kqueue, fd: RawFd) -> io::Result<()> {
        self.endpoint_mut(fd).enable_write(kq)
    }

    pub fn disable_write(&mut self, kq: &Kqueue, fd: RawFd) -> io::Result<()> {
        self.endpoint_mut(fd).disable_write(kq)
    }

    pub fn shutdown_write(&mut self, fd: RawFd) -> io::Result<()> {
        self.endpoint_mut(fd).shutdown_write()
    }

    /// READ Event 비활성화 후 eof 마킹
    pub fn mark_eof(&mut self, kq: &Kqueue, fd: RawFd) -> io::Result<()> {
        let endpoint = self.endpoint_mut(fd);
        endpoint.disable_read(kq)?;
        endpoint.read_eof = true;
        Ok(())
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
