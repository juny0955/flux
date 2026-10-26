use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    ptr,
};

use libc::{EV_ADD, EV_DISABLE, EV_ENABLE, EVFILT_READ, EVFILT_WRITE, uintptr_t};

/*
kevent(
    kq,          // 어느 kqueue에 요청할지
    changelist,  // 등록·변경할 설정 목록
    nchanges,    // 설정 개수
    eventlist,   // 발생한 이벤트를 받을 배열
    nevents,     // 배열에 받을 수 있는 최대 개수
    timeout      // 이벤트 대기 제한 시간
)
*/

pub struct Kqueue {
    kq: OwnedFd,
}

impl Kqueue {
    pub fn new() -> io::Result<Self> {
        // kqueue 생성
        let kq: i32 = unsafe { libc::kqueue() };
        if kq == -1 {
            return Err(io::Error::last_os_error());
        }

        let kq = unsafe { OwnedFd::from_raw_fd(kq) };
        // 소유권 이동
        Ok(Self { kq })
    }

    pub fn wait(&self, events: &mut [libc::kevent]) -> io::Result<usize> {
        let count = unsafe {
            libc::kevent(
                self.raw_fd(),
                ptr::null(),
                0,
                events.as_mut_ptr(),
                events.len() as i32,
                ptr::null(),
            )
        };

        if count == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(count as usize)
    }

    /// 해당 소켓 FD에 EVFILT_READ 이벤트를 kqueue에 등록한다
    pub fn enable_read(&self, socket_fd: RawFd) -> io::Result<()> {
        self.change_event(socket_fd, EV_ADD | EV_ENABLE, EVFILT_READ)
    }

    pub fn enable_write(&self, socket_fd: RawFd) -> io::Result<()> {
        self.change_event(socket_fd, EV_ADD | EV_ENABLE, EVFILT_WRITE)
    }

    pub fn disable_read(&self, socket_fd: RawFd) -> io::Result<()> {
        self.change_event(socket_fd, EV_DISABLE, EVFILT_READ)
    }

    pub fn disable_write(&self, socket_fd: RawFd) -> io::Result<()> {
        self.change_event(socket_fd, EV_DISABLE, EVFILT_WRITE)
    }

    pub fn raw_fd(&self) -> RawFd {
        self.kq.as_raw_fd()
    }

    fn change_event(&self, socket_fd: RawFd, flags: u16, filter: i16) -> io::Result<()> {
        let change = libc::kevent {
            ident: socket_fd as uintptr_t,
            fflags: 0,
            data: 0,
            udata: ptr::null_mut(),
            flags,
            filter,
        };

        let result =
            unsafe { libc::kevent(self.raw_fd(), &change, 1, ptr::null_mut(), 0, ptr::null()) };

        if result == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}
