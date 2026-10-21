use std::{
    error::Error,
    io,
    net::TcpListener,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    ptr,
    thread::{self},
};

use flux::proxy::handle_client;
use libc::{EV_ADD, EVFILT_READ, uintptr_t};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

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

fn main() -> Result<(), Box<dyn Error>> {
    setup();

    let listener = TcpListener::bind("127.0.0.1:9000")?;
    listener.set_nonblocking(true)?;
    info!("9000 port binding completed");

    let kq = create_kqueue()?;
    listener_reg_kqueue(listener.as_raw_fd(), kq.as_raw_fd())?;

    // listener 이벤트 받을 배열 준비
    let mut events = [libc::kevent {
        ident: 0,
        filter: 0,
        flags: 0,
        fflags: 0,
        data: 0,
        udata: ptr::null_mut(),
    }; 64];

    let listener_ident = uintptr_t::try_from(listener.as_raw_fd())?;
    loop {
        // event 받아올때 까지 대기함
        println!("listener event 대기");
        let count = unsafe {
            libc::kevent(
                kq.as_raw_fd(),
                ptr::null(),
                0,
                events.as_mut_ptr(),
                64,
                ptr::null(),
            )
        };
        if count == -1 {
            return Err(io::Error::last_os_error().into());
        }
        println!("listener event 수신");

        for event in &events[..count as usize] {
            if event.flags & libc::EV_ERROR != 0 {
                let code = i32::try_from(event.data)?;
                return Err(io::Error::from_raw_os_error(code).into());
            }

            if event.ident == listener_ident && event.filter == libc::EVFILT_READ {
                let (client, addr) = listener.accept()?;
                info!("client connected: {}", addr);

                thread::spawn(move || {
                    if let Err(e) = handle_client(client) {
                        error!("client {} error: {}", addr, e);
                    }

                    info!("client disconnected: {}", addr);
                });
            }
        }
    }
}

fn setup() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}

/// kqueue 생성 후 소유권
fn create_kqueue() -> Result<OwnedFd, io::Error> {
    // kqueue 생성
    let kq = unsafe { libc::kqueue() };
    if kq == -1 {
        return Err(io::Error::last_os_error());
    }

    // 소유권 이동
    Ok(unsafe { OwnedFd::from_raw_fd(kq) })
}

/// listener kqueue event 등록
fn listener_reg_kqueue(listener_fd: RawFd, kq_fd: RawFd) -> Result<(), Box<dyn Error>> {
    // listener event 설정 구조체
    let change = libc::kevent {
        ident: uintptr_t::try_from(listener_fd)?,
        filter: EVFILT_READ,
        flags: EV_ADD, // 추가
        fflags: 0,
        data: 0,
        udata: ptr::null_mut(),
    };

    let result = unsafe { libc::kevent(kq_fd, &change, 1, ptr::null_mut(), 0, ptr::null()) };
    if result == -1 {
        return Err(io::Error::last_os_error().into());
    }

    Ok(())
}
