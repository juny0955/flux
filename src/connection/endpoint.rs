use std::{
    io,
    net::{Shutdown, TcpStream},
    os::fd::{AsRawFd, RawFd},
};

use crate::kqueue::Kqueue;

pub(super) struct Endpoint {
    pub(super) stream: TcpStream,
    pub(super) read_enabled: bool,
    pub(super) write_enabled: bool,
    pub(super) read_eof: bool,
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
