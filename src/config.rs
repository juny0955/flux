use std::{
    error::Error,
    fmt, fs, io,
    net::{Ipv4Addr, SocketAddrV4},
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use clap::Parser;
use serde::{Deserialize, Deserializer};

const CONFIG_PATH: &str = "flux.toml";

#[derive(Debug, Parser)]
#[command(version, about = "TCP Proxy")]
pub struct Cli {
    /// 클라이언트 연결을 받을 IPv4 주소와 포트
    #[arg(
        short,
        long,
        help = "클라이언트 연결을 받을 IPv4 주소와 포트 [기본값: 127.0.0.1:9000]"
    )]
    pub listen: Option<SocketAddrV4>,

    /// 서버 IPv4 주소와 포트
    #[arg(short, long, help = "서버 IPv4 주소와 포트 [기본값: 127.0.0.1:8080]")]
    pub upstream: Option<SocketAddrV4>,

    /// 클라이언트 <-> 서버간 송수신 버퍼 크기
    #[arg(long, value_parser = parse_buffer_size, help = "방향별 버퍼 크기: 바이트 정수, KiB, MiB [기본값: 16KiB]")]
    pub buffer_size: Option<NonZeroUsize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen: Option<SocketAddrV4>,
    upstream: Option<SocketAddrV4>,
    #[serde(default, deserialize_with = "deserialize_buffer_size")]
    buffer_size: Option<NonZeroUsize>,
}

/// CLI 옵션, 파일 설정, 기본값 순서로 병합한 실행 설정.
#[derive(Debug)]
pub struct Config {
    pub listen: SocketAddrV4,
    pub upstream: SocketAddrV4,
    pub buffer_size: NonZeroUsize,
}

impl Config {
    /// 실행 작업 디렉터리의 `flux.toml`을 읽고 CLI 옵션을 우선 적용한다.
    /// 파일이 없으면 CLI 옵션과 기본값을 사용한다.
    ///
    /// # Errors
    /// 파일을 읽거나 설정값을 파싱할 수 없으면 경로와 원인을 반환한다.
    pub fn load(cli: Cli) -> Result<Self, Box<dyn Error>> {
        Self::load_from(cli, Path::new(CONFIG_PATH))
    }

    fn load_from(cli: Cli, path: &Path) -> Result<Self, Box<dyn Error>> {
        let file: FileConfig = match fs::read_to_string(path) {
            Ok(content) => toml::from_str(&content).map_err(|source| ConfigFileError {
                path: path.to_path_buf(),
                source: Box::new(source),
            })?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => FileConfig::default(),
            Err(source) => {
                return Err(ConfigFileError {
                    path: path.to_path_buf(),
                    source: Box::new(source),
                }
                .into());
            }
        };

        Ok(Self {
            listen: cli
                .listen
                .or(file.listen)
                .unwrap_or(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9000)),
            upstream: cli
                .upstream
                .or(file.upstream)
                .unwrap_or(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080)),
            buffer_size: cli
                .buffer_size
                .or(file.buffer_size)
                .unwrap_or(parse_buffer_size("16KiB")?),
        })
    }
}

#[derive(Debug)]
struct ConfigFileError {
    path: PathBuf,
    source: Box<dyn Error + Send + Sync>,
}

impl fmt::Display for ConfigFileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "설정 파일 {}: {}",
            self.path.display(),
            self.source
        )
    }
}

impl Error for ConfigFileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

fn deserialize_buffer_size<'de, D>(deserializer: D) -> Result<Option<NonZeroUsize>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_buffer_size(&value)
        .map(Some)
        .map_err(serde::de::Error::custom)
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
