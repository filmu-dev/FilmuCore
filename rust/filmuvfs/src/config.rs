use std::{
    env,
    path::PathBuf,
    process,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};

use crate::SERVICE_NAME;

pub const DEFAULT_L1_MAX_BYTES: u64 = 500 * 1024 * 1024;
pub const DEFAULT_L2_MAX_BYTES: u64 = 10 * 1024 * 1024 * 1024;
pub const DEFAULT_PREFETCH_MIN_CHUNKS: u32 = 4;
pub const DEFAULT_PREFETCH_MAX_CHUNKS: u32 = 16;
pub const DEFAULT_PREFETCH_STARTUP_CHUNKS: u32 = 8;
pub const DEFAULT_CHUNK_SIZE_SCAN_KB: usize = 8192;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MountAdapterKind {
    Auto,
    Fuse,
    Projfs,
    Winfsp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedMountAdapterKind {
    Fuse,
    Projfs,
    Winfsp,
}

impl MountAdapterKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Fuse => "fuse",
            Self::Projfs => "projfs",
            Self::Winfsp => "winfsp",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "fuse" | "fuse3" => Ok(Self::Fuse),
            "projfs" | "projectedfs" | "projected-filesystem" => Ok(Self::Projfs),
            "winfsp" => Ok(Self::Winfsp),
            other => bail!("unsupported mount adapter value: {other}"),
        }
    }

    #[must_use]
    pub fn default_for_platform() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::Fuse
        }

        #[cfg(target_os = "windows")]
        {
            Self::Projfs
        }

        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            Self::Auto
        }
    }

    pub fn resolve(self) -> Result<ResolvedMountAdapterKind> {
        match self {
            Self::Fuse => Ok(ResolvedMountAdapterKind::Fuse),
            Self::Projfs => Ok(ResolvedMountAdapterKind::Projfs),
            Self::Winfsp => Ok(ResolvedMountAdapterKind::Winfsp),
            Self::Auto => {
                #[cfg(target_os = "linux")]
                {
                    Ok(ResolvedMountAdapterKind::Fuse)
                }

                #[cfg(target_os = "windows")]
                {
                    Ok(ResolvedMountAdapterKind::Projfs)
                }

                #[cfg(not(any(target_os = "linux", target_os = "windows")))]
                {
                    bail!("FILMUVFS_MOUNT_ADAPTER=auto is unsupported on this host platform")
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheConfig {
    pub l1_max_bytes: u64,
    pub l2_enabled: bool,
    pub l2_path: PathBuf,
    pub l2_max_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefetchConfig {
    pub min_chunks: u32,
    pub max_chunks: u32,
    pub startup_chunks: u32,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            min_chunks: DEFAULT_PREFETCH_MIN_CHUNKS,
            max_chunks: DEFAULT_PREFETCH_MAX_CHUNKS,
            startup_chunks: DEFAULT_PREFETCH_STARTUP_CHUNKS,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SidecarConfig {
    pub service_name: String,
    pub daemon_id: String,
    pub session_id: String,
    pub mountpoint: PathBuf,
    pub mount_adapter: MountAdapterKind,
    pub allow_other: bool,
    pub grpc_endpoint: String,
    pub backend_http_base_url: Option<String>,
    pub backend_api_key: Option<String>,
    pub otlp_endpoint: Option<String>,
    pub log_filter: String,
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub reconnect_backoff_initial: Duration,
    pub reconnect_backoff_max: Duration,
    pub request_buffer: usize,
    pub cache: CacheConfig,
    pub prefetch: PrefetchConfig,
    pub prefetch_concurrency: usize,
    pub chunk_size_scan_kb: usize,
    pub chunk_size_random_kb: usize,
    pub windows_projfs_summary_interval: Duration,
}

impl SidecarConfig {
    pub fn from_env() -> Result<Self> {
        let default_daemon_id = format!("{}-{}", SERVICE_NAME, process::id());
        let mut daemon_id = env::var("FILMUVFS_DAEMON_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(default_daemon_id);
        let mut mountpoint = env::var("FILMUVFS_MOUNTPOINT")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from);
        let mut mount_adapter = env::var("FILMUVFS_MOUNT_ADAPTER")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| MountAdapterKind::parse(&value))
            .transpose()?
            .unwrap_or_else(MountAdapterKind::default_for_platform);
        let mut allow_other = parse_bool("FILMUVFS_ALLOW_OTHER", false)?;
        let mut grpc_endpoint = env::var("FILMUVFS_GRPC_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:50051".to_owned());
        let backend_http_base_url = env::var("FILMUVFS_BACKEND_HTTP_BASE_URL")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .map(|value| normalize_http_base_url(&value));
        let backend_api_key = env::var("FILMUVFS_BACKEND_API_KEY")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        let mut otlp_endpoint = env::var("FILMUVFS_OTLP_ENDPOINT")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let mut log_filter = env::var("FILMUVFS_LOG_FILTER")
            .unwrap_or_else(|_| "info,hyper=warn,hyper_util=warn,tonic=warn,h2=warn".to_owned());
        let mut cache_size_mb = parse_usize("FILMUVFS_CACHE_SIZE_MB", 500)?;
        let mut cache_l2_enabled = parse_bool("FILMUVFS_CACHE_L2_ENABLED", false)?;
        let mut cache_l2_path = env::var("FILMUVFS_CACHE_L2_PATH")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from);
        let mut cache_l2_max_bytes =
            parse_u64("FILMUVFS_CACHE_L2_MAX_BYTES", DEFAULT_L2_MAX_BYTES)?;
        let mut prefetch_min_chunks =
            parse_u32("FILMUVFS_PREFETCH_MIN_CHUNKS", DEFAULT_PREFETCH_MIN_CHUNKS)?;
        let mut prefetch_max_chunks =
            parse_u32("FILMUVFS_PREFETCH_MAX_CHUNKS", DEFAULT_PREFETCH_MAX_CHUNKS)?;
        let mut prefetch_startup_chunks = parse_u32(
            "FILMUVFS_PREFETCH_STARTUP_CHUNKS",
            DEFAULT_PREFETCH_STARTUP_CHUNKS,
        )?;
        let mut prefetch_concurrency = parse_usize("FILMUVFS_PREFETCH_CONCURRENCY", 4)?;
        let mut chunk_size_scan_kb =
            parse_usize("FILMUVFS_CHUNK_SIZE_SCAN_KB", DEFAULT_CHUNK_SIZE_SCAN_KB)?;
        let mut chunk_size_random_kb = parse_usize("FILMUVFS_CHUNK_SIZE_RANDOM_KB", 256)?;
        let mut windows_projfs_summary_interval_seconds = parse_u64(
            "FILMUVFS_WINDOWS_PROJFS_SUMMARY_INTERVAL_SECONDS",
            default_windows_projfs_summary_interval_seconds(),
        )?;

        let args: Vec<String> = env::args().skip(1).collect();
        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--mountpoint" => {
                    index += 1;
                    mountpoint = Some(PathBuf::from(required_arg_value(
                        &args,
                        index,
                        "--mountpoint",
                    )?));
                }
                "--grpc-server" => {
                    index += 1;
                    grpc_endpoint = required_arg_value(&args, index, "--grpc-server")?;
                }
                "--mount-adapter" => {
                    index += 1;
                    mount_adapter = MountAdapterKind::parse(&required_arg_value(
                        &args,
                        index,
                        "--mount-adapter",
                    )?)?;
                }
                "--allow-other" => {
                    allow_other = true;
                }
                "--otlp-endpoint" => {
                    index += 1;
                    otlp_endpoint = Some(required_arg_value(&args, index, "--otlp-endpoint")?);
                }
                "--daemon-id" => {
                    index += 1;
                    daemon_id = required_arg_value(&args, index, "--daemon-id")?;
                }
                "--log-filter" => {
                    index += 1;
                    log_filter = required_arg_value(&args, index, "--log-filter")?;
                }
                "--cache-size-mb" => {
                    index += 1;
                    cache_size_mb = required_arg_value(&args, index, "--cache-size-mb")?
                        .parse::<usize>()
                        .with_context(|| "failed to parse --cache-size-mb as usize")?;
                }
                "--cache-l2-enabled" => {
                    cache_l2_enabled = true;
                }
                "--cache-l2-path" => {
                    index += 1;
                    cache_l2_path = Some(PathBuf::from(required_arg_value(
                        &args,
                        index,
                        "--cache-l2-path",
                    )?));
                }
                "--cache-l2-max-bytes" => {
                    index += 1;
                    cache_l2_max_bytes = required_arg_value(&args, index, "--cache-l2-max-bytes")?
                        .parse::<u64>()
                        .with_context(|| "failed to parse --cache-l2-max-bytes as u64")?;
                }
                "--prefetch-min-chunks" => {
                    index += 1;
                    prefetch_min_chunks =
                        required_arg_value(&args, index, "--prefetch-min-chunks")?
                            .parse::<u32>()
                            .with_context(|| "failed to parse --prefetch-min-chunks as u32")?;
                }
                "--prefetch-max-chunks" => {
                    index += 1;
                    prefetch_max_chunks =
                        required_arg_value(&args, index, "--prefetch-max-chunks")?
                            .parse::<u32>()
                            .with_context(|| "failed to parse --prefetch-max-chunks as u32")?;
                }
                "--prefetch-startup-chunks" => {
                    index += 1;
                    prefetch_startup_chunks =
                        required_arg_value(&args, index, "--prefetch-startup-chunks")?
                            .parse::<u32>()
                            .with_context(|| "failed to parse --prefetch-startup-chunks as u32")?;
                }
                "--prefetch-concurrency" => {
                    index += 1;
                    prefetch_concurrency =
                        required_arg_value(&args, index, "--prefetch-concurrency")?
                            .parse::<usize>()
                            .with_context(|| "failed to parse --prefetch-concurrency as usize")?;
                }
                "--chunk-size-scan-kb" => {
                    index += 1;
                    chunk_size_scan_kb = required_arg_value(&args, index, "--chunk-size-scan-kb")?
                        .parse::<usize>()
                        .with_context(|| "failed to parse --chunk-size-scan-kb as usize")?;
                }
                "--chunk-size-random-kb" => {
                    index += 1;
                    chunk_size_random_kb =
                        required_arg_value(&args, index, "--chunk-size-random-kb")?
                            .parse::<usize>()
                            .with_context(|| "failed to parse --chunk-size-random-kb as usize")?;
                }
                "--windows-projfs-summary-interval-seconds" => {
                    index += 1;
                    windows_projfs_summary_interval_seconds = required_arg_value(
                        &args,
                        index,
                        "--windows-projfs-summary-interval-seconds",
                    )?
                    .parse::<u64>()
                    .with_context(|| {
                        "failed to parse --windows-projfs-summary-interval-seconds as u64"
                    })?;
                }
                "--help" | "-h" => {
                    print_usage();
                    process::exit(0);
                }
                other => {
                    bail!("unknown command-line argument: {other}");
                }
            }

            index += 1;
        }

        let mountpoint = mountpoint.context(
            "mountpoint is required; pass --mountpoint /mount or set FILMUVFS_MOUNTPOINT",
        )?;
        let _ = mount_adapter.resolve()?;
        ensure_positive_usize(cache_size_mb, "cache_size_mb")?;
        ensure_positive_u64(cache_l2_max_bytes, "cache.l2_max_bytes")?;
        ensure_positive_u32(prefetch_min_chunks, "prefetch.min_chunks")?;
        ensure_positive_u32(prefetch_max_chunks, "prefetch.max_chunks")?;
        ensure_positive_u32(prefetch_startup_chunks, "prefetch.startup_chunks")?;
        ensure_positive_usize(prefetch_concurrency, "prefetch_concurrency")?;
        ensure_positive_usize(chunk_size_scan_kb, "chunk_size_scan_kb")?;
        ensure_positive_usize(chunk_size_random_kb, "chunk_size_random_kb")?;
        if cache_l2_enabled && cache_l2_path.is_none() {
            bail!("cache.l2_path is required when L2 disk cache is enabled")
        }
        if prefetch_max_chunks < prefetch_min_chunks {
            bail!("prefetch.max_chunks must be greater than or equal to prefetch.min_chunks")
        }

        let cache = CacheConfig {
            l1_max_bytes: mebibytes_to_bytes(cache_size_mb)?,
            l2_enabled: cache_l2_enabled,
            l2_path: cache_l2_path.unwrap_or_default(),
            l2_max_bytes: cache_l2_max_bytes,
        };
        let prefetch = PrefetchConfig {
            min_chunks: prefetch_min_chunks,
            max_chunks: prefetch_max_chunks,
            startup_chunks: prefetch_startup_chunks,
        };

        Ok(Self {
            service_name: SERVICE_NAME.to_owned(),
            daemon_id: daemon_id.clone(),
            session_id: build_session_id(&daemon_id),
            mountpoint,
            mount_adapter,
            allow_other,
            grpc_endpoint: normalize_grpc_endpoint(&grpc_endpoint),
            backend_http_base_url,
            backend_api_key,
            otlp_endpoint,
            log_filter,
            connect_timeout: parse_duration_seconds("FILMUVFS_CONNECT_TIMEOUT_SECONDS", 5)?,
            rpc_timeout: parse_duration_seconds("FILMUVFS_RPC_TIMEOUT_SECONDS", 30)?,
            heartbeat_interval: parse_duration_seconds("FILMUVFS_HEARTBEAT_INTERVAL_SECONDS", 15)?,
            reconnect_backoff_initial: parse_duration_seconds(
                "FILMUVFS_RECONNECT_BACKOFF_INITIAL_SECONDS",
                1,
            )?,
            reconnect_backoff_max: parse_duration_seconds(
                "FILMUVFS_RECONNECT_BACKOFF_MAX_SECONDS",
                30,
            )?,
            request_buffer: parse_usize("FILMUVFS_REQUEST_BUFFER", 64)?,
            cache,
            prefetch,
            prefetch_concurrency,
            chunk_size_scan_kb,
            chunk_size_random_kb,
            windows_projfs_summary_interval: Duration::from_secs(
                windows_projfs_summary_interval_seconds,
            ),
        })
    }
}

fn build_session_id(daemon_id: &str) -> String {
    let epoch_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{daemon_id}-{epoch_millis}")
}

fn normalize_grpc_endpoint(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_owned()
    } else {
        format!("http://{trimmed}")
    }
}

fn normalize_http_base_url(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_owned()
}

fn print_usage() {
    println!(
        "usage: filmuvfs --mountpoint /mount --grpc-server localhost:50051 [--mount-adapter auto|fuse|projfs|winfsp] [--allow-other] [--otlp-endpoint http://localhost:4317] [--cache-size-mb 500] [--cache-l2-enabled --cache-l2-path ./filmuvfs-cache --cache-l2-max-bytes 10737418240] [--prefetch-min-chunks 4] [--prefetch-max-chunks 16] [--prefetch-startup-chunks 8] [--prefetch-concurrency 4] [--chunk-size-scan-kb 8192] [--chunk-size-random-kb 256] [--windows-projfs-summary-interval-seconds 300]"
    );
}

const fn default_windows_projfs_summary_interval_seconds() -> u64 {
    #[cfg(target_os = "windows")]
    {
        300
    }

    #[cfg(not(target_os = "windows"))]
    {
        0
    }
}

fn required_arg_value(args: &[String], index: usize, flag: &str) -> Result<String> {
    args.get(index)
        .cloned()
        .with_context(|| format!("missing value for {flag}"))
}

fn parse_duration_seconds(key: &str, default_seconds: u64) -> Result<Duration> {
    let raw = env::var(key).unwrap_or_else(|_| default_seconds.to_string());
    let seconds = raw.parse::<u64>().with_context(|| {
        format!("failed to parse {key} as an unsigned integer number of seconds")
    })?;
    Ok(Duration::from_secs(seconds))
}

fn parse_usize(key: &str, default_value: usize) -> Result<usize> {
    let raw = env::var(key).unwrap_or_else(|_| default_value.to_string());
    raw.parse::<usize>()
        .with_context(|| format!("failed to parse {key} as an unsigned integer"))
}

fn parse_u64(key: &str, default_value: u64) -> Result<u64> {
    let raw = env::var(key).unwrap_or_else(|_| default_value.to_string());
    raw.parse::<u64>()
        .with_context(|| format!("failed to parse {key} as an unsigned integer"))
}

fn parse_u32(key: &str, default_value: u32) -> Result<u32> {
    let raw = env::var(key).unwrap_or_else(|_| default_value.to_string());
    raw.parse::<u32>()
        .with_context(|| format!("failed to parse {key} as an unsigned integer"))
}

fn parse_bool(key: &str, default_value: bool) -> Result<bool> {
    let raw = env::var(key).unwrap_or_else(|_| default_value.to_string());
    parse_bool_value(&raw).with_context(|| format!("failed to parse {key} as a boolean value"))
}

fn parse_bool_value(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => bail!("unsupported boolean value: {other}"),
    }
}

fn ensure_positive_usize(value: usize, name: &str) -> Result<()> {
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(())
}

fn ensure_positive_u64(value: u64, name: &str) -> Result<()> {
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(())
}

fn ensure_positive_u32(value: u32, name: &str) -> Result<()> {
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        default_windows_projfs_summary_interval_seconds, MountAdapterKind, ResolvedMountAdapterKind,
    };

    #[test]
    fn parses_mount_adapter_aliases() {
        assert_eq!(
            MountAdapterKind::parse("auto").expect("adapter should parse"),
            MountAdapterKind::Auto
        );
        assert_eq!(
            MountAdapterKind::parse("fuse3").expect("adapter should parse"),
            MountAdapterKind::Fuse
        );
        assert_eq!(
            MountAdapterKind::parse("projectedfs").expect("adapter should parse"),
            MountAdapterKind::Projfs
        );
        assert_eq!(
            MountAdapterKind::parse("winfsp").expect("adapter should parse"),
            MountAdapterKind::Winfsp
        );
    }

    #[test]
    fn rejects_unknown_mount_adapter() {
        assert!(MountAdapterKind::parse("smb").is_err());
    }

    #[test]
    fn resolves_explicit_winfsp_adapter() {
        assert_eq!(
            MountAdapterKind::Winfsp
                .resolve()
                .expect("explicit winfsp should resolve"),
            ResolvedMountAdapterKind::Winfsp
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_summary_interval_defaults_to_five_minutes_on_windows() {
        assert_eq!(default_windows_projfs_summary_interval_seconds(), 300);
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn windows_summary_interval_defaults_to_disabled_on_non_windows() {
        assert_eq!(default_windows_projfs_summary_interval_seconds(), 0);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn resolves_auto_to_windows_projfs() {
        assert_eq!(
            MountAdapterKind::Auto
                .resolve()
                .expect("auto should resolve on Windows"),
            ResolvedMountAdapterKind::Projfs
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resolves_auto_to_linux_fuse() {
        assert_eq!(
            MountAdapterKind::Auto
                .resolve()
                .expect("auto should resolve on Linux"),
            ResolvedMountAdapterKind::Fuse
        );
    }
}

fn mebibytes_to_bytes(value: usize) -> Result<u64> {
    let value = u64::try_from(value).context("cache size exceeds u64")?;
    value
        .checked_mul(1024 * 1024)
        .context("cache size overflowed u64 when converting mebibytes to bytes")
}
