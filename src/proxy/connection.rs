use std::{
    io::{self, Error, ErrorKind, Read, Write},
    net::{Shutdown, TcpStream},
};

use tracing::info;

struct WriteBuffer {
    data: Vec<u8>,
    offset: usize,
}

impl WriteBuffer {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offset: 0,
        }
    }

    pub fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub fn advance(&mut self, len: usize) {
        self.offset += len;
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.offset = 0;
    }

    pub fn pending(&self) -> &[u8] {
        &self.data[self.offset..]
    }

    pub fn is_empty(&self) -> bool {
        self.offset == self.data.len()
    }
}

pub struct Connection {
    from: TcpStream,
    to: TcpStream,

    write_buffer: WriteBuffer,
}

impl Connection {
    pub fn new(from: TcpStream, to: TcpStream) -> io::Result<Self> {
        from.set_nonblocking(true)?;
        to.set_nonblocking(true)?;

        Ok(Self {
            from,
            to,
            write_buffer: WriteBuffer::new(),
        })
    }

    pub fn read(&mut self) -> io::Result<()> {
        let mut buf = [0u8; 8192];

        match self.from.read(&mut buf) {
            Ok(0) => {
                let _ = self.to.shutdown(Shutdown::Write);
                Ok(())
            }
            Ok(n) => {
                self.write_buffer.append(&buf[..n]);
                Ok(())
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                info!("read would block!");
                Ok(())
            }
            Err(e) => {
                let _ = self.to.shutdown(Shutdown::Both);
                let _ = self.from.shutdown(Shutdown::Both);
                Err(e)
            }
        }
    }

    pub fn write(&mut self) -> io::Result<()> {
        while !self.write_buffer.is_empty() {
            let pending = self.write_buffer.pending();

            match self.to.write(pending) {
                Ok(0) => {
                    return Err(Error::new(ErrorKind::WriteZero, "write zero bytes"));
                }
                Ok(n) => {
                    self.write_buffer.advance(n);

                    if self.write_buffer.is_empty() {
                        self.write_buffer.clear();
                    }
                }
                // 송신 버퍼 가득참
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    pub fn can_read(&self) -> bool {
        self.write_buffer.is_empty()
    }
}
