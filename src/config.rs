use std::{io, net::SocketAddrV4, num::NonZeroUsize};

use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about = "TCP Proxy")]
pub struct Config {
    /// 클라이언트 연결을 받을 IPv4 주소와 포트
    #[arg(short, long, default_value = "127.0.0.1:9000")]
    pub listen: SocketAddrV4,

    /// 서버 IPv4 주소와 포트
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    pub upstream: SocketAddrV4,

    /// 클라이언트 <-> 서버간 송수신 버퍼 크기
    #[arg(long, default_value = "16KiB", value_parser = parse_buffer_size)]
    pub buffer_size: NonZeroUsize,
}

fn parse_buffer_size(value: &str) -> Result<NonZeroUsize, io::Error> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "양의 정수 또는 KiB/MiB 단위로 입력하세요",
        )
    };

    let (number, multiplier) = match value.strip_suffix("KiB") {
        Some(number) => (number, 1024),
        None => match value.strip_suffix("MiB") {
            Some(number) => (number, 1024 * 1024),
            None => (value, 1),
        },
    };

    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }

    number
        .parse::<usize>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
        .filter(|size| isize::try_from(*size).is_ok())
        .and_then(NonZeroUsize::new)
        .ok_or_else(invalid)
}
