use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    ptr,
};

use libc::{EV_ADD, EV_DISABLE, EV_ENABLE, EVFILT_READ, EVFILT_WRITE, kevent, uintptr_t};

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
    pending_changes: Vec<kevent>,
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
        Ok(Self {
            kq,
            pending_changes: Vec::new(),
        })
    }

    pub fn wait(&mut self, events: &mut [libc::kevent]) -> io::Result<usize> {
        let count = unsafe {
            libc::kevent(
                self.raw_fd(),
                self.pending_changes.as_ptr(),
                self.pending_changes.len() as i32,
                events.as_mut_ptr(),
                events.len() as i32,
                ptr::null(),
            )
        };

        if count == -1 {
            return Err(io::Error::last_os_error());
        }

        self.pending_changes.clear();
        Ok(count as usize)
    }

    /// 해당 소켓 FD에 EVFILT_READ 이벤트를 kqueue에 등록한다
    pub fn enable_read(&mut self, socket_fd: RawFd) {
        self.pending_changes
            .push(event(socket_fd, EV_ADD | EV_ENABLE, EVFILT_READ))
    }

    pub fn enable_write(&mut self, socket_fd: RawFd) {
        self.pending_changes
            .push(event(socket_fd, EV_ADD | EV_ENABLE, EVFILT_WRITE))
    }

    pub fn disable_read(&mut self, socket_fd: RawFd) {
        self.pending_changes
            .push(event(socket_fd, EV_DISABLE, EVFILT_READ))
    }

    pub fn disable_write(&mut self, socket_fd: RawFd) {
        self.pending_changes
            .push(event(socket_fd, EV_DISABLE, EVFILT_WRITE))
    }

    pub fn cancel_changes(&mut self, fd: RawFd) {
        self.pending_changes
            .retain(|change| change.ident != fd as uintptr_t);
    }

    pub fn raw_fd(&self) -> RawFd {
        self.kq.as_raw_fd()
    }
}

fn event(socket_fd: RawFd, flags: u16, filter: i16) -> kevent {
    kevent {
        ident: socket_fd as uintptr_t,
        fflags: 0,
        data: 0,
        udata: ptr::null_mut(),
        flags,
        filter,
    }
}
