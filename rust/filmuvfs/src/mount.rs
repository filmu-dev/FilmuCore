use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use bytes::Bytes;
use dashmap::DashMap;
#[cfg(target_os = "linux")]
use fuse3 as _;
use thiserror::Error;
use tokio::sync::Notify;
#[cfg(any(target_os = "linux", target_os = "windows"))]
use tokio::{runtime::Handle, sync::oneshot, task::JoinHandle};
#[cfg(any(target_os = "linux", target_os = "windows"))]
use tokio_util::sync::CancellationToken;
use tonic::{transport::Endpoint, Request};
#[cfg(any(target_os = "linux", target_os = "windows"))]
use tracing::info;
use tracing::{debug, error, info_span, warn, Instrument};

#[cfg(any(target_os = "linux", target_os = "windows"))]
use crate::catalog::client::CatalogWatchRuntime;
#[cfg(target_os = "windows")]
use crate::windows_host::WindowsMountedFilesystem;
use crate::{
    cache::{CacheEngine, HybridCache, MemoryCache},
    catalog::state::{
        inode_for_entry_id as hashed_inode_for_entry_id, CatalogStateStore, ROOT_INODE,
    },
    chunk_engine::{
        ChunkCacheSnapshot, ChunkCoalescingSnapshot, ChunkEngine, ChunkEngineConfig,
        ChunkEngineError, ChunkReadRequest,
    },
    chunk_planner::ChunkPlannerConfig,
    config::{PrefetchConfig, ResolvedMountAdapterKind, SidecarConfig},
    hidden_paths::{is_hidden_path, is_ignored_path},
    media_path::{parse_media_semantic_path, MediaSemanticPathInfo},
    prefetch::{PrefetchSchedulerSnapshot, VelocityTracker},
    proto::{
        catalog_entry::Details as CatalogEntryDetails,
        filmu::vfs::catalog::v1::{
            filmu_vfs_catalog_service_client::FilmuVfsCatalogServiceClient,
            RefreshCatalogEntryRequest,
        },
        CatalogEntry, CatalogEntryKind, CatalogFileTransport, FileEntry,
    },
    telemetry::{
        record_backend_fallback, record_handle_startup_duration, record_inline_refresh,
        record_mounted_read_duration, record_prefetch_event, record_read_request,
        record_upstream_fetch_bytes, record_upstream_fetch_duration,
    },
    upstream::{UpstreamReadError, UpstreamReader},
};

pub const ROOT_PATH: &str = "/";

#[cfg(target_os = "linux")]
const ATTRIBUTE_TTL: std::time::Duration = std::time::Duration::from_secs(1);

const INLINE_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum MountRuntimeError {
    #[error("path {path} does not exist in the current catalog")]
    PathNotFound { path: String },
    #[error("inode {inode} does not exist in the current catalog")]
    InodeNotFound { inode: u64 },
    #[error("path {path} is not a directory")]
    NotDirectory { path: String },
    #[error("path {path} is not a file")]
    NotFile { path: String },
    #[error("catalog entry {entry_id} is missing the expected detail payload")]
    MissingDetails { entry_id: String },
    #[error("catalog entry {entry_id} does not expose an unrestricted URL")]
    MissingUrl { entry_id: String },
    #[error("handle {handle_id} does not exist")]
    HandleNotFound { handle_id: u64 },
    #[error(
        "handle {handle_id} is bound to inode {actual_inode}, but read requested inode {requested_inode}"
    )]
    HandleInodeMismatch {
        handle_id: u64,
        requested_inode: u64,
        actual_inode: u64,
    },
    #[error("name {name:?} is not valid UTF-8")]
    InvalidName { name: String },
    #[error("stale upstream URL for {path} returned status {status_code}")]
    StaleLease { path: String, status_code: u16 },
    #[error("I/O failure while reading {path}: {message}")]
    Io { path: String, message: String },
    #[error("read requested while mount is shutting down")]
    ShuttingDown,
}

impl MountRuntimeError {
    fn from_upstream(path: impl Into<String>, error: UpstreamReadError) -> Self {
        let path = path.into();
        match error {
            UpstreamReadError::StaleStatus { status } => Self::StaleLease {
                path,
                status_code: status.as_u16(),
            },
            other => Self::Io {
                path,
                message: other.to_string(),
            },
        }
    }

    fn from_chunk_engine(path: impl Into<String>, error: ChunkEngineError) -> Self {
        let path = path.into();
        match error {
            ChunkEngineError::Upstream(error) => Self::from_upstream(path, error),
            other => Self::Io {
                path,
                message: other.to_string(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountNodeKind {
    Directory,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountAttributes {
    pub inode: u64,
    pub entry_id: String,
    pub path: String,
    pub name: String,
    pub kind: MountNodeKind,
    pub size_bytes: u64,
    pub semantic_path: MediaSemanticPathInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountDirectoryEntry {
    pub inode: u64,
    pub entry_id: String,
    pub path: String,
    pub name: String,
    pub kind: MountNodeKind,
    pub size_bytes: u64,
    pub offset: i64,
    pub semantic_path: MediaSemanticPathInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountHandle {
    pub handle_id: u64,
    pub handle_key: String,
    pub inode: u64,
    pub entry_id: String,
    pub path: String,
    pub size_bytes: Option<u64>,
    pub semantic_path: MediaSemanticPathInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountReadRequest {
    pub handle_id: u64,
    pub handle_key: String,
    pub inode: u64,
    pub entry_id: String,
    pub item_id: String,
    pub item_external_ref: Option<String>,
    pub path: String,
    pub semantic_path: MediaSemanticPathInfo,
    pub unrestricted_url: String,
    pub provider_file_id: Option<String>,
    pub offset: u64,
    pub length: u32,
    pub size_bytes: Option<u64>,
    pub remote_direct: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MountHandleState {
    inode: u64,
    handle_key: String,
    invalidated: bool,
    opened_at: Instant,
    startup_recorded: bool,
}

#[derive(Debug)]
struct InlineRefreshFlight {
    notify: Notify,
    result: Mutex<Option<Option<String>>>,
}

impl InlineRefreshFlight {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            result: Mutex::new(None),
        }
    }

    fn finish(&self, result: Option<String>) {
        *self
            .result
            .lock()
            .expect("inline refresh flight lock poisoned") = Some(result);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> Option<String> {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self
                .result
                .lock()
                .expect("inline refresh flight lock poisoned")
                .clone()
            {
                return result;
            }
            notified.await;
        }
    }
}

#[tonic::async_trait]
pub trait CatalogEntryRefreshClient: Send + Sync {
    async fn refresh_catalog_entry(
        &self,
        provider_file_id: &str,
        handle_key: &str,
        entry_id: &str,
    ) -> std::result::Result<Option<String>, String>;
}

#[derive(Debug, Clone)]
pub struct GrpcCatalogEntryRefreshClient {
    endpoint: String,
    connect_timeout: Duration,
    rpc_timeout: Duration,
    heartbeat_interval: Duration,
}

impl GrpcCatalogEntryRefreshClient {
    pub fn new(
        endpoint: String,
        connect_timeout: Duration,
        rpc_timeout: Duration,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            endpoint,
            connect_timeout,
            rpc_timeout,
            heartbeat_interval,
        }
    }
}

#[tonic::async_trait]
impl CatalogEntryRefreshClient for GrpcCatalogEntryRefreshClient {
    async fn refresh_catalog_entry(
        &self,
        provider_file_id: &str,
        handle_key: &str,
        entry_id: &str,
    ) -> std::result::Result<Option<String>, String> {
        let endpoint = Endpoint::from_shared(self.endpoint.clone())
            .map_err(|error| error.to_string())?
            .connect_timeout(self.connect_timeout)
            .timeout(self.rpc_timeout)
            .tcp_nodelay(true)
            .tcp_keepalive(Some(self.heartbeat_interval))
            .http2_keep_alive_interval(self.heartbeat_interval)
            .keep_alive_timeout(self.rpc_timeout)
            .keep_alive_while_idle(true)
            .initial_connection_window_size(Some(1024 * 1024))
            .initial_stream_window_size(Some(1024 * 1024));
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| error.to_string())?;
        let mut client = FilmuVfsCatalogServiceClient::new(channel);
        let response = client
            .refresh_catalog_entry(Request::new(RefreshCatalogEntryRequest {
                provider_file_id: provider_file_id.to_owned(),
                handle_key: handle_key.to_owned(),
                entry_id: entry_id.to_owned(),
            }))
            .await
            .map_err(|error| error.to_string())?
            .into_inner();

        if response.success && !response.new_url.trim().is_empty() {
            Ok(Some(response.new_url))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct MountRuntime {
    catalog_state: Arc<CatalogStateStore>,
    chunk_engine: Arc<ChunkEngine>,
    prefetch_config: PrefetchConfig,
    scan_chunk_size_bytes: u32,
    refresh_client: Arc<RwLock<Option<Arc<dyn CatalogEntryRefreshClient>>>>,
    inline_refresh_flights: Arc<DashMap<String, Arc<InlineRefreshFlight>>>,
    handles: Arc<DashMap<u64, MountHandleState>>,
    handle_velocity: Arc<DashMap<u64, VelocityTracker>>,
    next_handle_id: Arc<AtomicU64>,
    active_reads: Arc<AtomicU64>,
    peak_open_handles: Arc<AtomicU64>,
    peak_active_reads: Arc<AtomicU64>,
    shutting_down: Arc<AtomicBool>,
    drain_notify: Arc<Notify>,
    session_id: Arc<String>,
    backend_http_base_url: Option<Arc<String>>,
    backend_api_key: Option<Arc<String>>,
}

impl std::fmt::Debug for MountRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MountRuntime")
            .field("open_handles", &self.handles.len())
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl MountRuntime {
    pub fn new(catalog_state: Arc<CatalogStateStore>, session_id: String) -> Self {
        Self::with_upstream_reader(catalog_state, session_id, UpstreamReader::new())
    }

    pub fn with_upstream_reader(
        catalog_state: Arc<CatalogStateStore>,
        session_id: String,
        upstream_reader: UpstreamReader,
    ) -> Self {
        let prefetch_config = PrefetchConfig::default();
        let chunk_engine = build_default_chunk_engine(upstream_reader, &prefetch_config);
        Self::with_chunk_engine(
            catalog_state,
            session_id,
            prefetch_config,
            ChunkPlannerConfig::default()
                .scan_chunk_size
                .min(u64::from(u32::MAX)) as u32,
            chunk_engine,
            None,
            None,
        )
    }

    pub fn with_sidecar_config_and_upstream_reader(
        catalog_state: Arc<CatalogStateStore>,
        session_id: String,
        config: &SidecarConfig,
        upstream_reader: UpstreamReader,
    ) -> Result<Self> {
        let chunk_engine = build_chunk_engine_from_sidecar_config(config, upstream_reader)?;
        Ok(Self::with_chunk_engine(
            catalog_state,
            session_id,
            config.prefetch.clone(),
            ((config.chunk_size_scan_kb as u64).saturating_mul(1024)).min(u64::from(u32::MAX))
                as u32,
            chunk_engine,
            config
                .backend_http_base_url
                .as_ref()
                .map(|value| Arc::new(value.clone())),
            config
                .backend_api_key
                .as_ref()
                .map(|value| Arc::new(value.clone())),
        ))
    }

    fn with_chunk_engine(
        catalog_state: Arc<CatalogStateStore>,
        session_id: String,
        prefetch_config: PrefetchConfig,
        scan_chunk_size_bytes: u32,
        chunk_engine: Arc<ChunkEngine>,
        backend_http_base_url: Option<Arc<String>>,
        backend_api_key: Option<Arc<String>>,
    ) -> Self {
        Self {
            catalog_state,
            chunk_engine,
            prefetch_config,
            scan_chunk_size_bytes,
            refresh_client: Arc::new(RwLock::new(None)),
            inline_refresh_flights: Arc::new(DashMap::new()),
            handles: Arc::new(DashMap::new()),
            handle_velocity: Arc::new(DashMap::new()),
            next_handle_id: Arc::new(AtomicU64::new(1)),
            active_reads: Arc::new(AtomicU64::new(0)),
            peak_open_handles: Arc::new(AtomicU64::new(0)),
            peak_active_reads: Arc::new(AtomicU64::new(0)),
            shutting_down: Arc::new(AtomicBool::new(false)),
            drain_notify: Arc::new(Notify::new()),
            session_id: Arc::new(session_id),
            backend_http_base_url,
            backend_api_key,
        }
    }

    pub fn set_refresh_client(&self, client: Arc<dyn CatalogEntryRefreshClient>) {
        *self
            .refresh_client
            .write()
            .expect("mount refresh client lock poisoned") = Some(client);
    }

    fn refresh_client(&self) -> Option<Arc<dyn CatalogEntryRefreshClient>> {
        self.refresh_client
            .read()
            .expect("mount refresh client lock poisoned")
            .clone()
    }

    fn assigned_inode_for_entry_id(&self, entry_id: &str) -> u64 {
        self.catalog_state
            .inode_for_entry_id(entry_id)
            .unwrap_or_else(|| hashed_inode_for_entry_id(entry_id))
    }

    fn assigned_inode_for_entry(&self, entry: &CatalogEntry) -> u64 {
        self.assigned_inode_for_entry_id(&entry.entry_id)
    }

    pub fn root_inode(&self) -> u64 {
        ROOT_INODE
    }

    pub fn open_handle_count(&self) -> usize {
        self.handles.len()
    }

    pub fn chunk_cache_weighted_size_bytes(&self) -> u64 {
        self.chunk_engine.cache_snapshot().weighted_size_bytes
    }

    pub fn chunk_cache_snapshot(&self) -> ChunkCacheSnapshot {
        self.chunk_engine.cache_snapshot()
    }

    pub fn prefetch_snapshot(&self) -> PrefetchSchedulerSnapshot {
        self.chunk_engine.prefetch_snapshot()
    }

    pub fn chunk_coalescing_snapshot(&self) -> ChunkCoalescingSnapshot {
        self.chunk_engine.chunk_coalescing_snapshot()
    }

    pub fn active_read_count(&self) -> u64 {
        self.active_reads.load(Ordering::SeqCst)
    }

    pub fn peak_open_handle_count(&self) -> u64 {
        self.peak_open_handles.load(Ordering::SeqCst)
    }

    pub fn peak_active_read_count(&self) -> u64 {
        self.peak_active_reads.load(Ordering::SeqCst)
    }

    pub fn active_handle_summaries(&self, limit: usize) -> Vec<String> {
        let mut summaries = self
            .handles
            .iter()
            .map(|entry| {
                let state = entry.value();
                let path = self
                    .entry_for_inode(state.inode)
                    .map(|catalog_entry| catalog_entry.path.clone())
                    .unwrap_or_else(|| format!("invalidated:inode:{}", state.inode));
                format!(
                    "{}|{}|{}|invalidated={}",
                    self.session_id.as_str(),
                    state.handle_key,
                    path,
                    state.invalidated
                )
            })
            .collect::<Vec<_>>();
        summaries.sort();
        summaries.truncate(limit);
        summaries
    }

    fn record_handle_startup_result(&self, handle_id: u64, result: &'static str) {
        let Some(mut handle_state) = self.handles.get_mut(&handle_id) else {
            return;
        };
        if handle_state.startup_recorded {
            return;
        }
        handle_state.startup_recorded = true;
        record_handle_startup_duration(handle_state.opened_at.elapsed(), result);
    }

    pub fn initiate_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        if self.active_reads.load(Ordering::SeqCst) == 0 {
            self.drain_notify.notify_waiters();
        }
    }

    pub async fn wait_for_reads_to_drain(&self) {
        while self.active_reads.load(Ordering::SeqCst) != 0 {
            self.drain_notify.notified().await;
        }
    }

    pub fn invalidate_handles_for_inodes(&self, inodes: &[u64]) -> usize {
        if inodes.is_empty() {
            return 0;
        }

        let removed_inodes: std::collections::HashSet<u64> = inodes.iter().copied().collect();
        let mut invalidated = 0;
        for mut handle in self.handles.iter_mut() {
            if removed_inodes.contains(&handle.inode) && !handle.invalidated {
                handle.invalidated = true;
                invalidated += 1;
            }
        }
        invalidated
    }

    pub fn getattr(&self, path: &str) -> Result<MountAttributes, MountRuntimeError> {
        let normalized_path = normalize_path(path);
        reject_hidden_runtime_path(&normalized_path)?;
        let entry = self
            .resolve_entry_by_runtime_path(&normalized_path)
            .ok_or_else(|| MountRuntimeError::PathNotFound {
                path: normalized_path.clone(),
            })?;
        let inode = self.assigned_inode_for_entry(&entry);
        self.attributes_from_entry(entry, inode)
    }

    pub fn getattr_by_inode(&self, inode: u64) -> Result<MountAttributes, MountRuntimeError> {
        let entry = self
            .entry_for_inode(inode)
            .ok_or(MountRuntimeError::InodeNotFound { inode })?;
        self.attributes_from_entry(entry, inode)
    }

    pub fn lookup_by_inode_name(
        &self,
        parent_inode: u64,
        name: &OsStr,
    ) -> Result<MountAttributes, MountRuntimeError> {
        let name = name
            .to_str()
            .ok_or_else(|| MountRuntimeError::InvalidName {
                name: name.to_string_lossy().into_owned(),
            })?;
        if is_hidden_path(name) {
            tracing::trace!(name = %name, "vfs.hidden_path.rejected");
            return Err(MountRuntimeError::PathNotFound {
                path: name.to_owned(),
            });
        }
        let parent =
            self.entry_for_inode(parent_inode)
                .ok_or(MountRuntimeError::InodeNotFound {
                    inode: parent_inode,
                })?;

        if parent.kind() != CatalogEntryKind::Directory {
            return Err(MountRuntimeError::NotDirectory { path: parent.path });
        }

        match name {
            "." => self.getattr_by_inode(parent_inode),
            ".." => self.getattr_by_inode(self.parent_inode_for_entry(&parent)),
            _ => {
                let child = self.lookup_child_entry(&parent, name).ok_or_else(|| {
                    MountRuntimeError::PathNotFound {
                        path: join_child_path(&parent.path, name),
                    }
                })?;
                let child_inode = self.assigned_inode_for_entry(&child);
                self.attributes_from_entry(child, child_inode)
            }
        }
    }

    pub fn readdir(&self, path: &str) -> Result<Vec<MountDirectoryEntry>, MountRuntimeError> {
        let normalized_path = normalize_path(path);
        reject_hidden_runtime_path(&normalized_path)?;
        let entry = self
            .resolve_entry_by_runtime_path(&normalized_path)
            .ok_or_else(|| MountRuntimeError::PathNotFound {
                path: normalized_path.clone(),
            })?;

        if entry.kind() != CatalogEntryKind::Directory {
            return Err(MountRuntimeError::NotDirectory {
                path: normalized_path,
            });
        }

        Ok(self.directory_entries_for_directory(entry))
    }

    pub fn readdir_by_inode(
        &self,
        inode: u64,
    ) -> Result<Vec<MountDirectoryEntry>, MountRuntimeError> {
        let entry = self
            .entry_for_inode(inode)
            .ok_or(MountRuntimeError::InodeNotFound { inode })?;

        if entry.kind() != CatalogEntryKind::Directory {
            return Err(MountRuntimeError::NotDirectory { path: entry.path });
        }

        Ok(self.directory_entries_for_directory(entry))
    }

    pub fn open(&self, path: &str) -> Result<MountHandle, MountRuntimeError> {
        let normalized_path = normalize_path(path);
        reject_hidden_runtime_path(&normalized_path)?;
        let entry = self
            .resolve_entry_by_runtime_path(&normalized_path)
            .ok_or_else(|| MountRuntimeError::PathNotFound {
                path: normalized_path.clone(),
            })?;
        self.open_entry(entry)
    }

    pub fn open_by_inode(&self, inode: u64) -> Result<MountHandle, MountRuntimeError> {
        let entry = self
            .entry_for_inode(inode)
            .ok_or(MountRuntimeError::InodeNotFound { inode })?;
        self.open_entry(entry)
    }

    pub fn prepare_read_request(
        &self,
        handle_id: u64,
        inode: u64,
        offset: u64,
        length: u32,
    ) -> Result<MountReadRequest, MountRuntimeError> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(MountRuntimeError::ShuttingDown);
        }

        let handle = self
            .handles
            .get(&handle_id)
            .map(|entry| entry.value().clone())
            .ok_or(MountRuntimeError::HandleNotFound { handle_id })?;

        if handle.inode != inode {
            return Err(MountRuntimeError::HandleInodeMismatch {
                handle_id,
                requested_inode: inode,
                actual_inode: handle.inode,
            });
        }

        if handle.invalidated {
            return Err(MountRuntimeError::InodeNotFound { inode });
        }

        let entry = self
            .entry_for_inode(inode)
            .ok_or(MountRuntimeError::InodeNotFound { inode })?;
        let file = file_details(&entry, &entry.path)?;
        let unrestricted_url =
            current_unrestricted_url(file).ok_or_else(|| MountRuntimeError::MissingUrl {
                entry_id: entry.entry_id.clone(),
            })?;
        let size_bytes = file.size_bytes.and_then(|size| u64::try_from(size).ok());
        let adjusted_length = match size_bytes {
            Some(total_size) if offset >= total_size => 0,
            Some(total_size) => {
                let remaining = total_size - offset;
                length.min(remaining.min(u64::from(u32::MAX)) as u32)
            }
            None => length,
        };

        Ok(MountReadRequest {
            handle_id,
            handle_key: handle.handle_key,
            inode,
            entry_id: entry.entry_id.clone(),
            item_id: file.item_id.clone(),
            item_external_ref: file.item_external_ref.clone(),
            path: entry.path.clone(),
            semantic_path: parse_media_semantic_path(
                &entry.path,
                file.item_external_ref.as_deref(),
            ),
            unrestricted_url,
            provider_file_id: file.provider_file_id.clone(),
            offset,
            length: adjusted_length,
            size_bytes,
            remote_direct: file.transport == CatalogFileTransport::RemoteDirect as i32,
        })
    }

    pub async fn read_bytes(
        &self,
        handle_id: u64,
        inode: u64,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, MountRuntimeError> {
        let request = self.prepare_read_request(handle_id, inode, offset, length)?;
        info!(
            handle_id = request.handle_id,
            inode = request.inode,
            path = %request.path,
            path_type = request.semantic_path.path_type.map(|value| value.as_str()).unwrap_or(""),
            tmdb_id = request.semantic_path.tmdb_id.as_deref().unwrap_or(""),
            tvdb_id = request.semantic_path.tvdb_id.as_deref().unwrap_or(""),
            imdb_id = request.semantic_path.imdb_id.as_deref().unwrap_or(""),
            season_number = ?request.semantic_path.season_number,
            episode_number = ?request.semantic_path.episode_number,
            offset = request.offset,
            requested_length = length,
            adjusted_length = request.length,
            file_size = ?request.size_bytes,
            "mount.read_bytes.request"
        );
        if request.length == 0 {
            record_read_request("ok");
            self.record_handle_startup_result(request.handle_id, "ok");
            return Ok(Bytes::new());
        }

        let chunk_request = build_chunk_read_request(&request);
        let preview = self.chunk_engine.preview_read(&chunk_request);
        let chunks_resolved = preview.chunks.len();
        if let Some(chunk) = preview
            .chunks
            .iter()
            .find(|chunk| chunk.length == 0 || chunk.length > 64 * 1024 * 1024)
        {
            warn!(
                path = %request.path,
                offset = request.offset,
                adjusted_length = request.length,
                chunk_offset = chunk.offset,
                chunk_length = chunk.length,
                file_size = ?request.size_bytes,
                "mount.read_bytes rejecting absurd planned chunk"
            );
            return Err(MountRuntimeError::Io {
                path: request.path.clone(),
                message: format!(
                    "planned chunk length {} at offset {} rejected",
                    chunk.length, chunk.offset
                ),
            });
        }
        let _active_read = ActiveReadGuard::new(self);
        let read_span = info_span!(
            "filmuvfs_read_request",
            session_id = %self.session_id,
            handle_key = %request.handle_key,
            provider_file_id = request.provider_file_id.as_deref().unwrap_or(""),
            entry_id = %request.entry_id,
            path = %request.path,
            path_type = request.semantic_path.path_type.map(|value| value.as_str()).unwrap_or(""),
            tmdb_id = request.semantic_path.tmdb_id.as_deref().unwrap_or(""),
            tvdb_id = request.semantic_path.tvdb_id.as_deref().unwrap_or(""),
            imdb_id = request.semantic_path.imdb_id.as_deref().unwrap_or(""),
            season_number = ?request.semantic_path.season_number,
            episode_number = ?request.semantic_path.episode_number,
            inode = request.inode,
            offset = request.offset,
            size = request.length,
        );

        async move {
            debug!(
                inode = request.inode,
                offset = request.offset,
                size = request.length,
                url = %request.unrestricted_url,
                "vfs.read.start"
            );

            let started_at = Instant::now();
            let result = self.chunk_engine.read(chunk_request.clone()).await;
            let elapsed = started_at.elapsed();
            record_upstream_fetch_duration(elapsed);

            match result {
                Ok(bytes) => {
                    record_read_request("ok");
                    record_mounted_read_duration(elapsed, "ok");
                    self.record_handle_startup_result(request.handle_id, "ok");
                    record_upstream_fetch_bytes(bytes.len() as u64);
                    self.schedule_adaptive_prefetch(&request, &chunk_request, bytes.len() as u64);
                    debug!(
                        inode = request.inode,
                        chunks_resolved,
                        bytes_returned = bytes.len(),
                        elapsed_seconds = elapsed.as_secs_f64(),
                        "vfs.read.complete"
                    );
                    Ok(bytes)
                }
                Err(error) if should_attempt_inline_refresh_for_error(&error) => {
                    let refresh_reason = inline_refresh_reason(&error);
                    warn!(
                        error = %error,
                        path = %request.path,
                        elapsed_seconds = elapsed.as_secs_f64(),
                        refresh_reason,
                        "attempting inline refresh before surfacing read failure"
                    );

                    if let Some(new_url) = self.attempt_inline_stale_refresh(&request).await {
                        let refreshed_request = ChunkReadRequest {
                            url: new_url.clone(),
                            ..chunk_request.clone()
                        };
                        let retry_prefetch_request = refreshed_request.clone();
                        let retry_started_at = Instant::now();
                        let retry_result = self.chunk_engine.read(refreshed_request).await;
                        let retry_elapsed = retry_started_at.elapsed();
                        record_upstream_fetch_duration(retry_elapsed);

                        match retry_result {
                            Ok(bytes) => {
                                record_read_request("ok");
                                record_mounted_read_duration(retry_elapsed, "ok");
                                self.record_handle_startup_result(request.handle_id, "ok");
                                record_upstream_fetch_bytes(bytes.len() as u64);
                                self.schedule_adaptive_prefetch(
                                    &request,
                                    &retry_prefetch_request,
                                    bytes.len() as u64,
                                );
                                debug!(
                                    inode = request.inode,
                                    chunks_resolved,
                                    bytes_returned = bytes.len(),
                                    elapsed_seconds = retry_elapsed.as_secs_f64(),
                                    path = %request.path,
                                    refreshed_url = %new_url,
                                    refresh_reason,
                                    "vfs.read.complete"
                                );
                                Ok(bytes)
                            }
                            Err(retry_error) if retry_error.is_stale() => {
                                record_read_request("estale");
                                record_mounted_read_duration(retry_elapsed, "estale");
                                self.record_handle_startup_result(request.handle_id, "estale");
                                warn!(
                                    error = %retry_error,
                                    path = %request.path,
                                    elapsed_seconds = retry_elapsed.as_secs_f64(),
                                    refreshed_url = %new_url,
                                    refresh_reason,
                                    "inline refresh returned another stale URL; returning ESTALE"
                                );
                                Err(MountRuntimeError::from_chunk_engine(
                                    request.path.clone(),
                                    retry_error,
                                ))
                            }
                            Err(retry_error) => {
                                if let Some(bytes) = self
                                    .try_backend_proxy_read(
                                        &request,
                                        &chunk_request,
                                        "post_inline_refresh_failure",
                                    )
                                    .await
                                    .map_err(|proxy_error| {
                                        record_read_request("error");
                                        record_mounted_read_duration(retry_elapsed, "error");
                                        self.record_handle_startup_result(
                                            request.handle_id,
                                            "error",
                                        );
                                        error!(
                                            error = %proxy_error,
                                            path = %request.path,
                                            refreshed_url = %new_url,
                                            refresh_reason,
                                            "backend HTTP fallback failed after inline refresh retry"
                                        );
                                        MountRuntimeError::from_chunk_engine(
                                            request.path.clone(),
                                            proxy_error,
                                        )
                                    })?
                                {
                                    record_read_request("ok");
                                    record_mounted_read_duration(retry_elapsed, "ok");
                                    self.record_handle_startup_result(request.handle_id, "ok");
                                    record_upstream_fetch_bytes(bytes.len() as u64);
                                    self.schedule_adaptive_prefetch(
                                        &request,
                                        &chunk_request,
                                        bytes.len() as u64,
                                    );
                                    Ok(bytes)
                                } else {
                                    record_read_request("error");
                                    record_mounted_read_duration(retry_elapsed, "error");
                                    self.record_handle_startup_result(request.handle_id, "error");
                                    error!(
                                        error = %retry_error,
                                        path = %request.path,
                                        elapsed_seconds = retry_elapsed.as_secs_f64(),
                                        refreshed_url = %new_url,
                                        refresh_reason,
                                        "direct upstream retry after inline refresh failed"
                                    );
                                    Err(MountRuntimeError::from_chunk_engine(
                                        request.path.clone(),
                                        retry_error,
                                    ))
                                }
                            }
                        }
                    } else {
                        if let Some(bytes) = self
                            .try_backend_proxy_read(
                                &request,
                                &chunk_request,
                                "inline_refresh_unavailable",
                            )
                            .await
                            .map_err(|proxy_error| {
                                let outcome = if proxy_error.is_stale() {
                                    "estale"
                                } else {
                                    "error"
                                };
                                record_read_request(outcome);
                                record_mounted_read_duration(elapsed, outcome);
                                self.record_handle_startup_result(request.handle_id, outcome);
                                MountRuntimeError::from_chunk_engine(
                                    request.path.clone(),
                                    proxy_error,
                                )
                            })?
                        {
                            record_read_request("ok");
                            record_mounted_read_duration(elapsed, "ok");
                            self.record_handle_startup_result(request.handle_id, "ok");
                            record_upstream_fetch_bytes(bytes.len() as u64);
                            self.schedule_adaptive_prefetch(
                                &request,
                                &chunk_request,
                                bytes.len() as u64,
                            );
                            Ok(bytes)
                        } else {
                            let outcome = if error.is_stale() { "estale" } else { "error" };
                            record_read_request(outcome);
                            record_mounted_read_duration(elapsed, outcome);
                            self.record_handle_startup_result(request.handle_id, outcome);
                            Err(MountRuntimeError::from_chunk_engine(
                                request.path.clone(),
                                error,
                            ))
                        }
                    }
                }
                Err(error) => {
                    if let Some(bytes) = self
                        .try_backend_proxy_read(&request, &chunk_request, "direct_read_failure")
                        .await
                        .map_err(|proxy_error| {
                            record_read_request("error");
                            record_mounted_read_duration(elapsed, "error");
                            self.record_handle_startup_result(request.handle_id, "error");
                            error!(
                                error = %proxy_error,
                                path = %request.path,
                                "backend HTTP fallback failed after direct upstream failure"
                            );
                            MountRuntimeError::from_chunk_engine(request.path.clone(), proxy_error)
                        })?
                    {
                        record_read_request("ok");
                        record_mounted_read_duration(elapsed, "ok");
                        self.record_handle_startup_result(request.handle_id, "ok");
                        record_upstream_fetch_bytes(bytes.len() as u64);
                        self.schedule_adaptive_prefetch(
                            &request,
                            &chunk_request,
                            bytes.len() as u64,
                        );
                        Ok(bytes)
                    } else {
                        record_read_request("error");
                        record_mounted_read_duration(elapsed, "error");
                        self.record_handle_startup_result(request.handle_id, "error");
                        error!(
                            error = %error,
                            path = %request.path,
                            elapsed_seconds = elapsed.as_secs_f64(),
                            "direct upstream read failed"
                        );
                        Err(MountRuntimeError::from_chunk_engine(request.path, error))
                    }
                }
            }
        }
        .instrument(read_span)
        .await
    }

    async fn attempt_inline_stale_refresh(&self, request: &MountReadRequest) -> Option<String> {
        let provider_file_id = request.provider_file_id.as_deref()?.trim();
        if provider_file_id.is_empty() {
            record_inline_refresh("skipped_missing_provider_file_id");
            return None;
        }

        if let Some(refreshed_url) = self.refreshed_catalog_url_for_request(request) {
            record_inline_refresh("reused_catalog_url");
            return Some(refreshed_url);
        }

        let refresh_client = self.refresh_client()?;
        let (flight, is_leader) = self.inline_refresh_flight(&request.entry_id);
        if !is_leader {
            record_inline_refresh("dedup_wait");
            return flight.wait().await;
        }

        let result = match tokio::time::timeout(
            INLINE_REFRESH_TIMEOUT,
            refresh_client.refresh_catalog_entry(
                provider_file_id,
                &request.handle_key,
                &request.entry_id,
            ),
        )
        .await
        {
            Ok(Ok(Some(new_url))) if !new_url.trim().is_empty() => {
                let _ = self
                    .catalog_state
                    .update_file_unrestricted_url(&request.entry_id, new_url.clone());
                record_inline_refresh("success");
                Some(new_url)
            }
            Ok(Ok(_)) => {
                record_inline_refresh("no_url");
                None
            }
            Ok(Err(_)) => {
                record_inline_refresh("error");
                None
            }
            Err(_) => {
                record_inline_refresh("timeout");
                None
            }
        };
        flight.finish(result.clone());
        self.inline_refresh_flights.remove(&request.entry_id);
        result
    }

    async fn try_backend_proxy_read(
        &self,
        request: &MountReadRequest,
        chunk_request: &ChunkReadRequest,
        reason: &'static str,
    ) -> Result<Option<Bytes>, ChunkEngineError> {
        let Some(proxy_url) = self.backend_stream_url(request) else {
            return Ok(None);
        };
        record_backend_fallback("attempt", reason);
        info!(
            entry_id = %request.entry_id,
            item_id = %request.item_id,
            path = %request.path,
            reason,
            proxy_url = %proxy_url,
            "attempting backend HTTP stream fallback"
        );
        let proxy_request = ChunkReadRequest {
            url: proxy_url,
            ..chunk_request.clone()
        };
        let bytes = match self.chunk_engine.read(proxy_request).await {
            Ok(bytes) => {
                record_backend_fallback("success", reason);
                bytes
            }
            Err(error) => {
                record_backend_fallback("failure", reason);
                return Err(error);
            }
        };
        Ok(Some(bytes))
    }

    fn backend_stream_url(&self, request: &MountReadRequest) -> Option<String> {
        if !request.remote_direct || request.item_id.trim().is_empty() {
            return None;
        }
        let base_url = self.backend_http_base_url.as_ref()?;
        let api_key = self.backend_api_key.as_ref()?;
        Some(format!(
            "{}/stream/file/{}?api_key={}",
            base_url.as_str(),
            request.item_id,
            api_key.as_str()
        ))
    }

    pub fn release(&self, handle_id: u64) -> Result<MountHandle, MountRuntimeError> {
        let (_key, handle) = self
            .handles
            .remove(&handle_id)
            .ok_or(MountRuntimeError::HandleNotFound { handle_id })?;
        self.handle_velocity.remove(&handle_id);
        self.chunk_engine.release_handle(&handle.handle_key);

        let current_entry = self.entry_for_inode(handle.inode);
        let entry_id = current_entry
            .as_ref()
            .map(|entry| entry.entry_id.clone())
            .unwrap_or_else(|| format!("invalidated:inode:{}", handle.inode));
        let path = current_entry
            .as_ref()
            .map(|entry| entry.path.clone())
            .unwrap_or_else(|| ROOT_PATH.to_owned());
        let size_bytes = current_entry.as_ref().and_then(|entry| {
            file_details(entry, &entry.path)
                .ok()
                .and_then(|file| file.size_bytes.and_then(|size| u64::try_from(size).ok()))
        });

        Ok(MountHandle {
            handle_id,
            handle_key: handle.handle_key,
            inode: handle.inode,
            entry_id,
            path,
            size_bytes,
            semantic_path: current_entry
                .as_ref()
                .map(|entry| self.semantic_path_info_for_entry(entry))
                .unwrap_or_default(),
        })
    }

    fn open_entry(&self, entry: CatalogEntry) -> Result<MountHandle, MountRuntimeError> {
        let inode = self.assigned_inode_for_entry(&entry);
        let file = file_details(&entry, &entry.path)?;
        let size_bytes = file.size_bytes.and_then(|size| u64::try_from(size).ok());
        let handle_id = self.next_handle_id.fetch_add(1, Ordering::Relaxed);
        let handle_key = format!("{}:{handle_id}", self.session_id);

        self.handles.insert(
            handle_id,
            MountHandleState {
                inode,
                handle_key: handle_key.clone(),
                invalidated: false,
                opened_at: Instant::now(),
                startup_recorded: false,
            },
        );
        update_max(&self.peak_open_handles, self.handles.len() as u64);
        self.chunk_engine.register_handle(&handle_key);
        let semantic_path = self.semantic_path_info_for_entry(&entry);

        Ok(MountHandle {
            handle_id,
            handle_key,
            inode,
            entry_id: entry.entry_id,
            path: entry.path,
            size_bytes,
            semantic_path,
        })
    }

    fn schedule_adaptive_prefetch(
        &self,
        request: &MountReadRequest,
        chunk_request: &ChunkReadRequest,
        bytes_read: u64,
    ) {
        let adaptive_chunks = {
            let mut tracker = self
                .handle_velocity
                .entry(request.handle_id)
                .or_insert_with(|| {
                    VelocityTracker::new(
                        self.prefetch_config.min_chunks,
                        self.prefetch_config.max_chunks,
                    )
                });
            tracker.update(request.offset, bytes_read)
        };

        if adaptive_chunks == 0 {
            return;
        }
        record_prefetch_event("adaptive_scheduled");

        let engine = Arc::clone(&self.chunk_engine);
        let prefetch_request = chunk_request.clone();
        tokio::spawn(async move {
            if let Err(error) = engine
                .prefetch_ahead(prefetch_request.clone(), adaptive_chunks)
                .await
            {
                record_prefetch_event("adaptive_error");
                tracing::debug!(
                    error = %error,
                    handle_key = %prefetch_request.handle_key,
                    adaptive_chunks,
                    "adaptive prefetch scheduling failed"
                );
            }
        });
    }

    pub fn spawn_startup_prefetch(&self, handle: &MountHandle, runtime_handle: &Handle) {
        let Some(entry) = self.entry_for_inode(handle.inode) else {
            return;
        };
        let Ok(file) = file_details(&entry, &entry.path) else {
            return;
        };
        let Some(unrestricted_url) = current_unrestricted_url(file) else {
            return;
        };
        let provider_file_id = file.provider_file_id.clone();
        let startup_window_bytes = u64::from(self.scan_chunk_size_bytes)
            .saturating_mul(u64::from(self.prefetch_config.startup_chunks.max(1)));
        let request_length = match handle.size_bytes {
            Some(0) => 0,
            Some(size_bytes) => {
                size_bytes.min(startup_window_bytes.min(u64::from(u32::MAX))) as u32
            }
            None => startup_window_bytes.min(u64::from(u32::MAX)) as u32,
        };
        if request_length == 0 {
            return;
        }

        let engine = Arc::clone(&self.chunk_engine);
        let request = ChunkReadRequest {
            handle_key: handle.handle_key.clone(),
            file_id: handle.entry_id.clone(),
            url: unrestricted_url,
            provider_file_id,
            offset: 0,
            length: request_length,
            file_size: handle.size_bytes,
        };

        runtime_handle.spawn(async move {
            if let Err(error) = engine.prefetch_request(request.clone()).await {
                record_prefetch_event("startup_error");
                tracing::debug!(
                    error = %error,
                    handle_key = %request.handle_key,
                    "startup prefetch scheduling failed"
                );
            } else {
                record_prefetch_event("startup_scheduled");
            }
        });
    }

    fn entry_for_inode(&self, inode: u64) -> Option<CatalogEntry> {
        self.catalog_state.entry_by_inode(inode)
    }

    fn parent_inode_for_entry(&self, entry: &CatalogEntry) -> u64 {
        self.parent_entry_for_entry(entry)
            .map(|parent| self.assigned_inode_for_entry(&parent))
            .unwrap_or(ROOT_INODE)
    }

    fn parent_entry_for_entry(&self, entry: &CatalogEntry) -> Option<CatalogEntry> {
        if let Some(parent) = entry
            .parent_entry_id
            .as_deref()
            .and_then(|entry_id| self.catalog_state.entry(entry_id))
        {
            return Some(parent);
        }

        parent_path_from_catalog_path(&entry.path)
            .and_then(|parent_path| self.catalog_state.entry_by_path(parent_path))
    }

    fn directory_entries_for_directory(&self, entry: CatalogEntry) -> Vec<MountDirectoryEntry> {
        let inode = self.assigned_inode_for_entry(&entry);
        let parent_inode = self.parent_inode_for_entry(&entry);
        let parent_entry = self.parent_entry_for_entry(&entry);
        let parent_path = parent_entry
            .as_ref()
            .map(|parent| parent.path.clone())
            .unwrap_or_else(|| ROOT_PATH.to_owned());
        let current_semantic_path = self.semantic_path_info_for_entry(&entry);
        let parent_semantic_path = parent_entry
            .as_ref()
            .map(|entry| self.semantic_path_info_for_entry(entry))
            .unwrap_or_else(|| parse_media_semantic_path(&parent_path, None));

        let mut directory_entries = vec![
            MountDirectoryEntry {
                inode,
                entry_id: entry.entry_id.clone(),
                path: entry.path.clone(),
                name: ".".to_owned(),
                kind: MountNodeKind::Directory,
                size_bytes: 0,
                offset: 1,
                semantic_path: current_semantic_path,
            },
            MountDirectoryEntry {
                inode: parent_inode,
                entry_id: entry
                    .parent_entry_id
                    .clone()
                    .or_else(|| parent_entry.as_ref().map(|parent| parent.entry_id.clone()))
                    .unwrap_or_else(|| entry.entry_id.clone()),
                path: parent_path,
                name: "..".to_owned(),
                kind: MountNodeKind::Directory,
                size_bytes: 0,
                offset: 2,
                semantic_path: parent_semantic_path,
            },
        ];

        let visible_children: Vec<CatalogEntry> = self
            .catalog_state
            .children_of(&entry.entry_id)
            .into_iter()
            .filter(|child| !path_should_be_hidden(&child.path) && !is_hidden_path(&child.name))
            .collect();

        directory_entries.extend(visible_children.iter().cloned().enumerate().map(
            |(index, child)| {
                let child_inode = self.assigned_inode_for_entry(&child);
                self.directory_entry_from_catalog_entry(child, child_inode, index as i64 + 3)
            },
        ));
        let next_offset = directory_entries.len() as i64 + 1;
        directory_entries.extend(self.alternate_directory_entries(
            &entry,
            &visible_children,
            next_offset,
        ));

        directory_entries
    }

    fn resolve_entry_by_runtime_path(&self, path: &str) -> Option<CatalogEntry> {
        let normalized_path = normalize_path(path);
        if normalized_path == ROOT_PATH {
            return self.catalog_state.entry_by_path(ROOT_PATH);
        }

        let mut current = self.catalog_state.entry_by_path(ROOT_PATH)?;
        for segment in normalized_path
            .split('/')
            .filter(|segment| !segment.is_empty())
        {
            current = self.lookup_child_entry(&current, segment)?;
        }
        Some(current)
    }

    fn lookup_child_entry(&self, parent: &CatalogEntry, name: &str) -> Option<CatalogEntry> {
        let children = self.catalog_state.children_of(&parent.entry_id);
        if let Some(exact_child) = children.iter().find(|entry| entry.name == name) {
            return Some(exact_child.clone());
        }
        self.semantic_child_match(parent, name, &children)
    }

    fn semantic_child_match(
        &self,
        parent: &CatalogEntry,
        name: &str,
        children: &[CatalogEntry],
    ) -> Option<CatalogEntry> {
        let requested_alias = SemanticChildAlias::from_parent_and_name(
            self.semantic_path_info_for_entry(parent),
            parent,
            name,
        )?;
        let mut matches = children
            .iter()
            .filter(|child| requested_alias.matches(self, child))
            .cloned();
        let first = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(first)
    }

    fn directory_descendant_external_refs(&self, entry: &CatalogEntry) -> Vec<String> {
        let mut refs = Vec::new();
        self.collect_descendant_external_refs(entry, &mut refs);
        refs.sort();
        refs.dedup();
        refs
    }

    fn collect_descendant_external_refs(&self, entry: &CatalogEntry, refs: &mut Vec<String>) {
        if let Some(CatalogEntryDetails::File(file)) = entry.details.as_ref() {
            if let Some(external_ref) = file.item_external_ref.as_ref() {
                let trimmed = external_ref.trim();
                if !trimmed.is_empty() {
                    refs.push(trimmed.to_owned());
                }
            }
            return;
        }

        for child in self.catalog_state.children_of(&entry.entry_id) {
            self.collect_descendant_external_refs(&child, refs);
        }
    }

    fn attributes_from_entry(
        &self,
        entry: CatalogEntry,
        inode: u64,
    ) -> Result<MountAttributes, MountRuntimeError> {
        let kind = entry.kind();
        let size_bytes = match kind {
            CatalogEntryKind::Directory => 0,
            CatalogEntryKind::File => file_details(&entry, &entry.path)?
                .size_bytes
                .and_then(|size| u64::try_from(size).ok())
                .unwrap_or(0),
            CatalogEntryKind::Unspecified => {
                return Err(MountRuntimeError::MissingDetails {
                    entry_id: entry.entry_id,
                });
            }
        };
        let semantic_path = self.semantic_path_info_for_entry(&entry);

        Ok(MountAttributes {
            inode,
            entry_id: entry.entry_id,
            path: entry.path,
            name: entry.name,
            kind: match kind {
                CatalogEntryKind::Directory => MountNodeKind::Directory,
                CatalogEntryKind::File | CatalogEntryKind::Unspecified => MountNodeKind::File,
            },
            size_bytes,
            semantic_path,
        })
    }

    fn directory_entry_from_catalog_entry(
        &self,
        entry: CatalogEntry,
        inode: u64,
        offset: i64,
    ) -> MountDirectoryEntry {
        let kind = entry.kind();
        let size_bytes = match kind {
            CatalogEntryKind::Directory => 0,
            CatalogEntryKind::File => file_details(&entry, &entry.path)
                .ok()
                .and_then(|file| file.size_bytes.and_then(|size| u64::try_from(size).ok()))
                .unwrap_or(0),
            CatalogEntryKind::Unspecified => 0,
        };
        let semantic_path = self.semantic_path_info_for_entry(&entry);

        MountDirectoryEntry {
            inode,
            entry_id: entry.entry_id,
            path: entry.path,
            name: entry.name,
            kind: match kind {
                CatalogEntryKind::Directory => MountNodeKind::Directory,
                CatalogEntryKind::File | CatalogEntryKind::Unspecified => MountNodeKind::File,
            },
            size_bytes,
            offset,
            semantic_path,
        }
    }

    fn alias_directory_entry_from_catalog_entry(
        &self,
        parent: &CatalogEntry,
        entry: CatalogEntry,
        inode: u64,
        alias_name: String,
        offset: i64,
    ) -> MountDirectoryEntry {
        let kind = entry.kind();
        let size_bytes = match kind {
            CatalogEntryKind::Directory => 0,
            CatalogEntryKind::File => file_details(&entry, &entry.path)
                .ok()
                .and_then(|file| file.size_bytes.and_then(|size| u64::try_from(size).ok()))
                .unwrap_or(0),
            CatalogEntryKind::Unspecified => 0,
        };
        let semantic_path = self.semantic_path_info_for_entry(&entry);

        MountDirectoryEntry {
            inode,
            entry_id: entry.entry_id,
            path: join_child_path(&parent.path, &alias_name),
            name: alias_name,
            kind: match kind {
                CatalogEntryKind::Directory => MountNodeKind::Directory,
                CatalogEntryKind::File | CatalogEntryKind::Unspecified => MountNodeKind::File,
            },
            size_bytes,
            offset,
            semantic_path,
        }
    }

    fn semantic_path_info_for_entry(&self, entry: &CatalogEntry) -> MediaSemanticPathInfo {
        let inherited_external_ref = match entry.details.as_ref() {
            Some(CatalogEntryDetails::File(file)) => file.item_external_ref.clone(),
            _ => {
                let descendant_refs = self.directory_descendant_external_refs(entry);
                if descendant_refs.len() == 1 {
                    descendant_refs.into_iter().next()
                } else {
                    None
                }
            }
        };

        parse_media_semantic_path(&entry.path, inherited_external_ref.as_deref())
    }

    fn refreshed_catalog_url_for_request(&self, request: &MountReadRequest) -> Option<String> {
        let entry = self.catalog_state.entry(&request.entry_id)?;
        let file = file_details(&entry, &entry.path).ok()?;
        let current_url = current_unrestricted_url(file)?;
        if current_url == request.unrestricted_url {
            return None;
        }
        Some(current_url)
    }

    fn inline_refresh_flight(&self, entry_id: &str) -> (Arc<InlineRefreshFlight>, bool) {
        match self.inline_refresh_flights.entry(entry_id.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => (Arc::clone(entry.get()), false),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let flight = Arc::new(InlineRefreshFlight::new());
                entry.insert(Arc::clone(&flight));
                (flight, true)
            }
        }
    }

    fn alternate_directory_entries(
        &self,
        parent: &CatalogEntry,
        children: &[CatalogEntry],
        starting_offset: i64,
    ) -> Vec<MountDirectoryEntry> {
        if children.is_empty() {
            return Vec::new();
        }

        let canonical_names: HashSet<String> =
            children.iter().map(|child| child.name.clone()).collect();
        let alias_sets: Vec<Vec<String>> = children
            .iter()
            .map(|child| self.child_alias_names(parent, child))
            .collect();
        let mut alias_counts: HashMap<String, usize> = HashMap::new();
        for aliases in &alias_sets {
            for alias in aliases {
                if canonical_names.contains(alias) {
                    continue;
                }
                *alias_counts.entry(alias.clone()).or_insert(0) += 1;
            }
        }

        let mut alias_entries = Vec::new();
        let mut offset = starting_offset;
        for (child, aliases) in children.iter().zip(alias_sets.into_iter()) {
            for alias in aliases {
                if canonical_names.contains(&alias) {
                    continue;
                }
                if alias_counts.get(&alias).copied() != Some(1) {
                    continue;
                }
                let child_inode = self.assigned_inode_for_entry(child);
                alias_entries.push(self.alias_directory_entry_from_catalog_entry(
                    parent,
                    child.clone(),
                    child_inode,
                    alias,
                    offset,
                ));
                offset += 1;
            }
        }

        alias_entries
    }

    fn child_alias_names(&self, parent: &CatalogEntry, child: &CatalogEntry) -> Vec<String> {
        let parent_semantic = self.semantic_path_info_for_entry(parent);
        let child_semantic = self.semantic_path_info_for_entry(child);
        let mut aliases = Vec::new();

        if matches!(parent.path.as_str(), "/shows" | "/movies") {
            if let Some(alias) = self.external_ref_alias_for_entry(child) {
                aliases.push(alias);
            }
        }

        if matches!(
            parent_semantic.path_type,
            Some(crate::media_path::MediaSemanticPathType::ShowDirectory)
        ) {
            match child_semantic.path_type {
                Some(crate::media_path::MediaSemanticPathType::ShowSeasonDirectory) => {
                    if let Some(season_number) = child_semantic.season_number {
                        aliases.push(format!("Season {:02}", season_number));
                    }
                }
                Some(crate::media_path::MediaSemanticPathType::ShowSpecialsDirectory) => {
                    aliases.push("Specials".to_owned());
                }
                _ => {}
            }
        }

        if matches!(
            parent_semantic.path_type,
            Some(crate::media_path::MediaSemanticPathType::ShowSeasonDirectory)
                | Some(crate::media_path::MediaSemanticPathType::ShowSpecialsDirectory)
        ) {
            if let Some(episode_number) = child_semantic.episode_number {
                aliases.push(self.episode_alias_name(child, episode_number));
            }
        }

        aliases.sort();
        aliases.dedup();
        aliases.retain(|alias| alias != &child.name && !is_hidden_path(alias));
        aliases
    }

    fn external_ref_alias_for_entry(&self, entry: &CatalogEntry) -> Option<String> {
        let descendant_refs = self.directory_descendant_external_refs(entry);
        if descendant_refs.len() != 1 {
            return None;
        }
        format_external_ref_alias(descendant_refs.first()?)
    }

    fn episode_alias_name(&self, entry: &CatalogEntry, episode_number: u32) -> String {
        let extension = Path::new(&entry.name)
            .extension()
            .and_then(|extension| extension.to_str())
            .filter(|extension| !extension.is_empty());
        match extension {
            Some(extension) => format!("Episode {:02}.{extension}", episode_number),
            None => format!("Episode {:02}", episode_number),
        }
    }
}

fn build_default_chunk_engine(
    upstream_reader: UpstreamReader,
    prefetch_config: &PrefetchConfig,
) -> Arc<ChunkEngine> {
    let cache: Arc<dyn CacheEngine> =
        Arc::new(MemoryCache::new(crate::config::DEFAULT_L1_MAX_BYTES));
    let planner = ChunkPlannerConfig {
        sequential_prefetch_chunks: prefetch_config.min_chunks as usize,
        ..ChunkPlannerConfig::default()
    };
    Arc::new(
        ChunkEngine::new(
            cache,
            ChunkEngineConfig {
                planner,
                prefetch_max_background_per_handle: prefetch_config.max_background_per_handle,
                ..ChunkEngineConfig::default()
            },
            upstream_reader,
        )
        .expect("default chunk engine configuration should be valid"),
    )
}

fn build_chunk_engine_from_sidecar_config(
    config: &SidecarConfig,
    upstream_reader: UpstreamReader,
) -> Result<Arc<ChunkEngine>> {
    let cache = build_cache_engine(&config.cache)?;
    let mut planner = ChunkPlannerConfig::with_chunk_sizes(
        (config.chunk_size_scan_kb as u64).saturating_mul(1024),
        (config.chunk_size_random_kb as u64).saturating_mul(1024),
    );
    planner.sequential_prefetch_chunks = config.prefetch.min_chunks as usize;
    let chunk_config = ChunkEngineConfig {
        planner,
        prefetch_concurrency: config.prefetch_concurrency,
        prefetch_max_background_per_handle: config.prefetch.max_background_per_handle,
    };

    Ok(Arc::new(ChunkEngine::new(
        cache,
        chunk_config,
        upstream_reader,
    )?))
}

fn build_cache_engine(config: &crate::config::CacheConfig) -> Result<Arc<dyn CacheEngine>> {
    if config.l2_enabled {
        Ok(Arc::new(HybridCache::new(
            config.l1_max_bytes,
            config.l2_path.clone(),
            config.l2_max_bytes,
        )?))
    } else {
        Ok(Arc::new(MemoryCache::new(config.l1_max_bytes)))
    }
}

struct ActiveReadGuard<'a> {
    runtime: &'a MountRuntime,
}

impl<'a> ActiveReadGuard<'a> {
    fn new(runtime: &'a MountRuntime) -> Self {
        let active_reads = runtime.active_reads.fetch_add(1, Ordering::SeqCst) + 1;
        update_max(&runtime.peak_active_reads, active_reads);
        Self { runtime }
    }
}

impl Drop for ActiveReadGuard<'_> {
    fn drop(&mut self) {
        if self.runtime.active_reads.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.runtime.drain_notify.notify_waiters();
        }
    }
}

fn update_max(target: &AtomicU64, candidate: u64) {
    let _ = target.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
        (candidate > current).then_some(candidate)
    });
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
pub struct Session {
    cancel: CancellationToken,
    watch_task: JoinHandle<Result<()>>,
    mounted_filesystem: PlatformMountedFilesystem,
    mount_runtime: Arc<MountRuntime>,
}

#[cfg(target_os = "linux")]
type PlatformMountedFilesystem = UnixMountedFilesystem;

#[cfg(target_os = "windows")]
type PlatformMountedFilesystem = WindowsMountedFilesystem;

#[cfg(any(target_os = "linux", target_os = "windows"))]
impl Session {
    pub async fn mount<P: AsRef<Path>>(
        mountpoint: P,
        grpc_addr: String,
        mut config: SidecarConfig,
        catalog_state: Arc<CatalogStateStore>,
        mount_runtime: Arc<MountRuntime>,
    ) -> Result<Self> {
        config.grpc_endpoint = grpc_addr;

        let resolved_adapter = config.mount_adapter.resolve()?;
        #[cfg(target_os = "linux")]
        if resolved_adapter != ResolvedMountAdapterKind::Fuse {
            bail!("the selected mount adapter is not supported on Linux hosts");
        }

        prepare_mountpoint(mountpoint.as_ref(), resolved_adapter)?;

        let mount_span = info_span!(
            "filmuvfs_mount_start",
            session_id = %config.session_id,
            daemon_id = %config.daemon_id,
            mountpoint = %mountpoint.as_ref().display(),
            grpc_server = %config.grpc_endpoint,
            mount_adapter = ?resolved_adapter,
        );

        async move {
            let cancel = CancellationToken::new();
            mount_runtime.set_refresh_client(Arc::new(GrpcCatalogEntryRefreshClient::new(
                config.grpc_endpoint.clone(),
                config.connect_timeout,
                config.rpc_timeout,
                config.heartbeat_interval,
            )));
            let watch_runtime = CatalogWatchRuntime::new(
                config.clone(),
                Arc::clone(&catalog_state),
                Some(Arc::clone(&mount_runtime)),
            );
            let (initial_sync_tx, initial_sync_rx) =
                oneshot::channel::<std::result::Result<(), String>>();
            let watch_cancel = cancel.clone();
            let watch_task = tokio::spawn(async move {
                watch_runtime
                    .run_until_cancelled(watch_cancel, Some(initial_sync_tx))
                    .await
            });

            let initial_sync = tokio::time::timeout(config.rpc_timeout, initial_sync_rx)
                .await
                .map_err(|_| anyhow::anyhow!("timed out waiting for initial catalog snapshot"))?
                .map_err(|_| anyhow::anyhow!("catalog watch task ended before initial snapshot"))?;

            match initial_sync {
                Ok(()) => {
                    info!(
                        session_id = %config.session_id,
                        mountpoint = %mountpoint.as_ref().display(),
                        mount_adapter = ?resolved_adapter,
                        "initial catalog snapshot received; mounting host adapter"
                    );
                }
                Err(message) => {
                    cancel.cancel();
                    let _ = watch_task.await;
                    bail!(message);
                }
            }

            #[cfg(target_os = "linux")]
            let mounted_filesystem = match Arc::clone(&mount_runtime)
                .mount_filesystem(
                    mountpoint.as_ref(),
                    &config.service_name,
                    config.allow_other,
                )
                .await
            {
                Ok(filesystem) => filesystem,
                Err(error) => {
                    cancel.cancel();
                    let _ = watch_task.await;
                    return Err(error.into());
                }
            };

            #[cfg(target_os = "windows")]
            let mounted_filesystem = match Arc::clone(&mount_runtime)
                .mount_windows_filesystem(
                    mountpoint.as_ref(),
                    &config.service_name,
                    config.allow_other,
                    resolved_adapter,
                )
                .await
            {
                Ok(filesystem) => filesystem,
                Err(error) => {
                    cancel.cancel();
                    let _ = watch_task.await;
                    return Err(error.into());
                }
            };

            info!(
                session_id = %config.session_id,
                mountpoint = %mounted_filesystem.mount_path().display(),
                mount_adapter = ?resolved_adapter,
                "host filesystem adapter mounted successfully"
            );

            Ok(Self {
                cancel,
                watch_task,
                mounted_filesystem,
                mount_runtime,
            })
        }
        .instrument(mount_span)
        .await
    }

    pub async fn shutdown(self) -> Result<()> {
        self.mount_runtime.initiate_shutdown();
        self.cancel.cancel();
        self.mount_runtime.wait_for_reads_to_drain().await;
        self.mounted_filesystem.unmount().await?;

        let watch_result = self.watch_task.await;
        match watch_result {
            Ok(result) => result,
            Err(join_error) => Err(join_error.into()),
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub struct Session;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
impl Session {
    pub async fn mount<P: AsRef<Path>>(
        _mountpoint: P,
        _grpc_addr: String,
        _config: SidecarConfig,
        _catalog_state: Arc<CatalogStateStore>,
        _mount_runtime: Arc<MountRuntime>,
    ) -> Result<Self> {
        bail!("filmuvfs mount session is only supported on Linux and Windows hosts")
    }

    pub async fn shutdown(self) -> Result<()> {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod linux_fuse {
    use std::{
        ffi::OsString,
        num::NonZeroU32,
        path::{Path, PathBuf},
        sync::Arc,
        time::SystemTime,
    };

    use async_stream::stream;
    use fuse3::{
        raw::{prelude::*, MountHandle as RawMountHandle, Session as RawSession},
        Errno, MountOptions, Result as FuseResult, Timestamp,
    };
    use tracing::debug;

    use super::{
        file_type_from_node_kind, MountAttributes, MountNodeKind, MountRuntime, MountRuntimeError,
        ATTRIBUTE_TTL,
    };

    #[derive(Debug)]
    pub struct UnixMountedFilesystem {
        mount_path: PathBuf,
        handle: RawMountHandle,
    }

    impl UnixMountedFilesystem {
        pub fn mount_path(&self) -> &Path {
            &self.mount_path
        }

        pub async fn unmount(self) -> std::io::Result<()> {
            self.handle.unmount().await
        }
    }

    #[derive(Debug, Clone)]
    struct FuseFilesystemAdapter {
        runtime: Arc<MountRuntime>,
    }

    impl FuseFilesystemAdapter {
        fn new(runtime: Arc<MountRuntime>) -> Self {
            Self { runtime }
        }
    }

    impl MountRuntime {
        pub async fn mount_filesystem<P: AsRef<Path>>(
            self: Arc<Self>,
            mount_path: P,
            service_name: &str,
            allow_other: bool,
        ) -> std::io::Result<UnixMountedFilesystem> {
            let mount_path = mount_path.as_ref().to_path_buf();
            let mut mount_options = MountOptions::default();
            mount_options.fs_name(service_name).read_only(true);
            if allow_other {
                mount_options.allow_other(true);
            }

            let adapter = FuseFilesystemAdapter::new(self);
            let session = RawSession::new(mount_options);
            let handle = session.mount(adapter, &mount_path).await?;

            Ok(UnixMountedFilesystem { mount_path, handle })
        }
    }

    impl Filesystem for FuseFilesystemAdapter {
        async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
            Ok(ReplyInit {
                max_write: NonZeroU32::new(1024 * 1024).expect("non-zero max write"),
            })
        }

        async fn statfs(&self, _req: Request, _inode: u64) -> FuseResult<ReplyStatFs> {
            let counts = self.runtime.catalog_state.counts();
            let entry_count = (counts.directories + counts.files) as u64;
            Ok(ReplyStatFs {
                blocks: entry_count.max(1),
                bfree: 0,
                bavail: 0,
                files: entry_count.max(1),
                ffree: 0,
                bsize: 4096,
                namelen: 255,
                frsize: 4096,
            })
        }

        async fn destroy(&self, _req: Request) {
            debug!("destroying fuse3 mount session");
        }

        async fn lookup(
            &self,
            _req: Request,
            parent: u64,
            name: &std::ffi::OsStr,
        ) -> FuseResult<ReplyEntry> {
            let attributes = self
                .runtime
                .lookup_by_inode_name(parent, name)
                .map_err(errno_from_mount_error)?;
            Ok(reply_entry_from_attributes(attributes))
        }

        async fn getattr(
            &self,
            _req: Request,
            inode: u64,
            _fh: Option<u64>,
            _flags: u32,
        ) -> FuseResult<ReplyAttr> {
            let attributes = self
                .runtime
                .getattr_by_inode(inode)
                .map_err(errno_from_mount_error)?;
            Ok(ReplyAttr {
                ttl: ATTRIBUTE_TTL,
                attr: file_attr_from_attributes(&attributes),
            })
        }

        async fn open(&self, _req: Request, inode: u64, _flags: u32) -> FuseResult<ReplyOpen> {
            let handle = self
                .runtime
                .open_by_inode(inode)
                .map_err(errno_from_mount_error)?;
            Ok(ReplyOpen {
                fh: handle.handle_id,
                flags: 1u32, // FOPEN_DIRECT_IO — bypass kernel page cache for ChunkEngine-managed reads
            })
        }

        async fn read(
            &self,
            _req: Request,
            inode: u64,
            fh: u64,
            offset: u64,
            size: u32,
        ) -> FuseResult<ReplyData> {
            let bytes = self
                .runtime
                .read_bytes(fh, inode, offset, size)
                .await
                .map_err(errno_from_mount_error)?;
            Ok(ReplyData { data: bytes })
        }

        async fn release(
            &self,
            _req: Request,
            _inode: u64,
            fh: u64,
            _flags: u32,
            _lock_owner: u64,
            _flush: bool,
        ) -> FuseResult<()> {
            self.runtime.release(fh).map_err(errno_from_mount_error)?;
            Ok(())
        }

        async fn opendir(&self, _req: Request, inode: u64, _flags: u32) -> FuseResult<ReplyOpen> {
            let attributes = self
                .runtime
                .getattr_by_inode(inode)
                .map_err(errno_from_mount_error)?;
            if attributes.kind != MountNodeKind::Directory {
                return Err(libc::ENOTDIR.into());
            }

            Ok(ReplyOpen {
                fh: inode,
                flags: 0,
            })
        }

        async fn readdir<'a>(
            &'a self,
            _req: Request,
            parent: u64,
            _fh: u64,
            offset: i64,
        ) -> FuseResult<
            ReplyDirectory<
                impl futures_util::Stream<Item = FuseResult<DirectoryEntry>> + Send + 'a,
            >,
        > {
            let entries = self
                .runtime
                .readdir_by_inode(parent)
                .map_err(errno_from_mount_error)?;

            let start_index = if offset <= 0 { 0 } else { offset as usize };
            let reply_entries: Vec<FuseResult<DirectoryEntry>> = entries
                .into_iter()
                .skip(start_index)
                .map(|entry| {
                    Ok(DirectoryEntry {
                        inode: entry.inode,
                        kind: file_type_from_node_kind(entry.kind),
                        name: OsString::from(entry.name),
                        offset: entry.offset,
                    })
                })
                .collect();

            Ok(ReplyDirectory {
                entries: stream! {
                    for entry in reply_entries {
                        yield entry;
                    }
                },
            })
        }

        async fn readdirplus<'a>(
            &'a self,
            _req: Request,
            parent: u64,
            _fh: u64,
            offset: u64,
            _lock_owner: u64,
        ) -> FuseResult<
            ReplyDirectoryPlus<
                impl futures_util::Stream<Item = FuseResult<DirectoryEntryPlus>> + Send + 'a,
            >,
        > {
            let entries = self
                .runtime
                .readdir_by_inode(parent)
                .map_err(errno_from_mount_error)?;

            let start_index = offset as usize;
            let reply_entries: Vec<FuseResult<DirectoryEntryPlus>> = entries
                .into_iter()
                .skip(start_index)
                .map(|entry| {
                    let attributes = self
                        .runtime
                        .getattr_by_inode(entry.inode)
                        .map_err(errno_from_mount_error)?;
                    Ok(DirectoryEntryPlus {
                        inode: entry.inode,
                        generation: 0,
                        kind: file_type_from_node_kind(entry.kind),
                        name: OsString::from(entry.name),
                        offset: entry.offset,
                        attr: file_attr_from_attributes(&attributes),
                        entry_ttl: ATTRIBUTE_TTL,
                        attr_ttl: ATTRIBUTE_TTL,
                    })
                })
                .collect();

            Ok(ReplyDirectoryPlus {
                entries: stream! {
                    for entry in reply_entries {
                        yield entry;
                    }
                },
            })
        }

        async fn releasedir(
            &self,
            _req: Request,
            _inode: u64,
            _fh: u64,
            _flags: u32,
        ) -> FuseResult<()> {
            Ok(())
        }

        async fn access(&self, _req: Request, inode: u64, _mask: u32) -> FuseResult<()> {
            self.runtime
                .getattr_by_inode(inode)
                .map_err(errno_from_mount_error)?;
            Ok(())
        }
    }

    fn errno_from_mount_error(error: MountRuntimeError) -> Errno {
        match error {
            MountRuntimeError::PathNotFound { .. } | MountRuntimeError::InodeNotFound { .. } => {
                Errno::new_not_exist()
            }
            MountRuntimeError::NotDirectory { .. } => Errno::new_is_not_dir(),
            MountRuntimeError::NotFile { .. } => Errno::new_is_dir(),
            MountRuntimeError::HandleNotFound { .. }
            | MountRuntimeError::HandleInodeMismatch { .. } => Errno::from(libc::EBADF),
            MountRuntimeError::MissingDetails { .. }
            | MountRuntimeError::MissingUrl { .. }
            | MountRuntimeError::Io { .. }
            | MountRuntimeError::ShuttingDown => Errno::from(libc::EIO),
            MountRuntimeError::InvalidName { .. } => Errno::from(libc::EINVAL),
            MountRuntimeError::StaleLease { .. } => Errno::from(libc::ESTALE),
        }
    }

    fn reply_entry_from_attributes(attributes: MountAttributes) -> ReplyEntry {
        ReplyEntry {
            ttl: ATTRIBUTE_TTL,
            attr: file_attr_from_attributes(&attributes),
            generation: 0,
        }
    }

    fn file_attr_from_attributes(attributes: &MountAttributes) -> FileAttr {
        let timestamp: Timestamp = SystemTime::UNIX_EPOCH.into();
        let is_directory = matches!(attributes.kind, MountNodeKind::Directory);
        let size = if is_directory {
            0
        } else {
            attributes.size_bytes
        };

        FileAttr {
            ino: attributes.inode,
            size,
            blocks: if size == 0 { 0 } else { size.div_ceil(512) },
            atime: timestamp,
            mtime: timestamp,
            ctime: timestamp,
            kind: file_type_from_node_kind(attributes.kind),
            perm: if is_directory { 0o555 } else { 0o444 },
            nlink: if is_directory { 2 } else { 1 },
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            rdev: 0,
            blksize: 4096,
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_fuse::UnixMountedFilesystem;

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == ROOT_PATH {
        return ROOT_PATH.to_owned();
    }

    let normalized_segments: Vec<&str> = trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    format!("/{}", normalized_segments.join("/"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SemanticChildAlias {
    ExternalRef {
        prefix: Option<String>,
        value: String,
    },
    Season {
        season_number: u32,
    },
    Episode {
        season_number: Option<u32>,
        episode_number: u32,
    },
}

impl SemanticChildAlias {
    fn from_parent_and_name(
        parent_semantic: MediaSemanticPathInfo,
        parent: &CatalogEntry,
        name: &str,
    ) -> Option<Self> {
        let trimmed_name = name.trim();
        if trimmed_name.is_empty() {
            return None;
        }

        if parent.path == "/shows" || parent.path == "/movies" {
            if let Some((prefix, value)) = parse_external_ref_alias(trimmed_name) {
                return Some(Self::ExternalRef { prefix, value });
            }
        }

        if matches!(
            parent_semantic.path_type,
            Some(crate::media_path::MediaSemanticPathType::ShowDirectory)
        ) {
            if let Some(season_number) = parse_season_alias(trimmed_name) {
                return Some(Self::Season { season_number });
            }
        }

        if matches!(
            parent_semantic.path_type,
            Some(crate::media_path::MediaSemanticPathType::ShowSeasonDirectory)
                | Some(crate::media_path::MediaSemanticPathType::ShowSpecialsDirectory)
        ) {
            if let Some(episode_number) = parse_episode_alias(trimmed_name) {
                return Some(Self::Episode {
                    season_number: parent_semantic.season_number,
                    episode_number,
                });
            }
        }

        None
    }

    fn matches(&self, runtime: &MountRuntime, child: &CatalogEntry) -> bool {
        match self {
            Self::ExternalRef { prefix, value } => runtime
                .directory_descendant_external_refs(child)
                .into_iter()
                .any(|external_ref| {
                    external_ref_matches_alias(&external_ref, prefix.as_deref(), value)
                }),
            Self::Season { season_number } => {
                let child_semantic = runtime.semantic_path_info_for_entry(child);
                child_semantic.season_number == Some(*season_number)
            }
            Self::Episode {
                season_number,
                episode_number,
            } => {
                let child_semantic = runtime.semantic_path_info_for_entry(child);
                child_semantic.episode_number == Some(*episode_number)
                    && (season_number.is_none() || child_semantic.season_number == *season_number)
            }
        }
    }
}

fn file_details<'a>(
    entry: &'a CatalogEntry,
    path: &str,
) -> Result<&'a FileEntry, MountRuntimeError> {
    if entry.kind() != CatalogEntryKind::File {
        return Err(MountRuntimeError::NotFile {
            path: path.to_owned(),
        });
    }

    match entry.details.as_ref() {
        Some(CatalogEntryDetails::File(file)) => Ok(file),
        _ => Err(MountRuntimeError::MissingDetails {
            entry_id: entry.entry_id.clone(),
        }),
    }
}

fn current_unrestricted_url(file: &FileEntry) -> Option<String> {
    file.unrestricted_url
        .clone()
        .filter(|url| !url.trim().is_empty())
}

fn should_attempt_inline_refresh_for_error(error: &ChunkEngineError) -> bool {
    matches!(
        error,
        ChunkEngineError::Upstream(_) | ChunkEngineError::InvalidChunkPayload { .. }
    )
}

fn inline_refresh_reason(error: &ChunkEngineError) -> &'static str {
    match error {
        ChunkEngineError::Upstream(UpstreamReadError::StaleStatus { .. }) => "stale_status",
        ChunkEngineError::Upstream(UpstreamReadError::UnexpectedStatus { .. }) => {
            "unexpected_status"
        }
        ChunkEngineError::Upstream(UpstreamReadError::InvalidUrl { .. }) => "invalid_url",
        ChunkEngineError::Upstream(UpstreamReadError::BuildRequest { .. }) => "build_request",
        ChunkEngineError::Upstream(UpstreamReadError::Network { .. }) => "network",
        ChunkEngineError::Upstream(UpstreamReadError::ReadBody { .. }) => "read_body",
        ChunkEngineError::InvalidChunkPayload { .. } => "invalid_chunk_payload",
        ChunkEngineError::InvalidRequest { .. } => "invalid_request",
        ChunkEngineError::Scheduler(_) => "scheduler",
        ChunkEngineError::IncompleteCoverage => "incomplete_coverage",
    }
}

fn build_chunk_read_request(request: &MountReadRequest) -> ChunkReadRequest {
    let provider_file_id = request
        .provider_file_id
        .as_ref()
        .map(|id| id.trim())
        .filter(|id| !id.is_empty())
        .map(|id| id.to_owned());
    ChunkReadRequest {
        handle_key: request.handle_key.clone(),
        // Cache identity must stay unique per catalog entry. Some providers reuse low-cardinality
        // file ids (for example "1") across unrelated titles, which poisons chunk reuse.
        file_id: request.entry_id.clone(),
        url: request.unrestricted_url.clone(),
        provider_file_id,
        offset: request.offset,
        length: request.length,
        file_size: request.size_bytes,
    }
}

fn join_child_path(parent: &str, child_name: &str) -> String {
    if parent == ROOT_PATH {
        format!("/{child_name}")
    } else {
        format!("{parent}/{child_name}")
    }
}

fn parent_path_from_catalog_path(path: &str) -> Option<&str> {
    if path == ROOT_PATH {
        return None;
    }

    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() || trimmed == ROOT_PATH {
        return Some(ROOT_PATH);
    }

    match trimmed.rfind('/') {
        Some(0) => Some(ROOT_PATH),
        Some(index) => Some(&trimmed[..index]),
        None => Some(ROOT_PATH),
    }
}

fn reject_hidden_runtime_path(path: &str) -> Result<(), MountRuntimeError> {
    if path_should_be_hidden(path) {
        tracing::trace!(path = %path, "vfs.hidden_path.rejected");
        return Err(MountRuntimeError::PathNotFound {
            path: path.to_owned(),
        });
    }

    Ok(())
}

fn path_should_be_hidden(path: &str) -> bool {
    if is_ignored_path(path) {
        return true;
    }

    normalize_path(path)
        .split('/')
        .filter(|segment| !segment.is_empty())
        .any(is_hidden_path)
}

fn parse_external_ref_alias(name: &str) -> Option<(Option<String>, String)> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some((prefix, value)) = trimmed.split_once('-') {
        let normalized_prefix = prefix.trim().to_ascii_lowercase();
        if matches!(normalized_prefix.as_str(), "tmdb" | "tvdb" | "imdb") {
            let normalized_value = value.trim();
            if !normalized_value.is_empty() {
                return Some((Some(normalized_prefix), normalized_value.to_owned()));
            }
        }
    }

    Some((None, trimmed.to_owned()))
}

fn external_ref_matches_alias(
    external_ref: &str,
    expected_prefix: Option<&str>,
    expected_value: &str,
) -> bool {
    let trimmed = external_ref.trim();
    let Some((prefix, value)) = trimmed.split_once(':') else {
        return false;
    };
    let normalized_prefix = prefix.trim().to_ascii_lowercase();
    let normalized_value = value.trim();
    if normalized_value.is_empty() {
        return false;
    }

    if let Some(expected_prefix) = expected_prefix {
        return normalized_prefix == expected_prefix && normalized_value == expected_value;
    }
    normalized_value == expected_value
}

fn format_external_ref_alias(external_ref: &str) -> Option<String> {
    let trimmed = external_ref.trim();
    let (prefix, value) = trimmed.split_once(':')?;
    let normalized_prefix = prefix.trim().to_ascii_lowercase();
    let normalized_value = value.trim();
    if normalized_value.is_empty() {
        return None;
    }
    if !matches!(normalized_prefix.as_str(), "tmdb" | "tvdb" | "imdb") {
        return None;
    }
    Some(format!("{normalized_prefix}-{normalized_value}"))
}

fn parse_season_alias(name: &str) -> Option<u32> {
    let normalized = normalize_alias_token(name);
    if normalized == "specials" {
        return Some(0);
    }
    if let Some(value) = parse_prefixed_alias_number(&normalized, &["season", "s"], 2) {
        return Some(value);
    }
    None
}

fn parse_episode_alias(name: &str) -> Option<u32> {
    let normalized = normalize_alias_token(name);
    if let Some((_, episode_number)) = parse_normalized_season_episode_alias(&normalized) {
        return Some(episode_number);
    }
    if let Some((_, episode_number)) = parse_normalized_x_notation_alias(&normalized) {
        return Some(episode_number);
    }
    if let Some(value) = parse_prefixed_alias_number(&normalized, &["episode", "ep", "e"], 3) {
        return Some(value);
    }
    None
}

fn parse_normalized_season_episode_alias(normalized: &str) -> Option<(u32, u32)> {
    let bytes = normalized.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b's' {
            index += 1;
            continue;
        }
        let Some(season_end) = consume_alias_digits(bytes, index + 1, 2) else {
            index += 1;
            continue;
        };
        if season_end >= bytes.len() || bytes[season_end] != b'e' {
            index += 1;
            continue;
        }
        let Some(episode_end) = consume_alias_digits(bytes, season_end + 1, 3) else {
            index += 1;
            continue;
        };
        let season_number = normalized[index + 1..season_end].parse::<u32>().ok()?;
        let episode_number = normalized[season_end + 1..episode_end]
            .parse::<u32>()
            .ok()?;
        return Some((season_number, episode_number));
    }
    None
}

fn parse_normalized_x_notation_alias(normalized: &str) -> Option<(u32, u32)> {
    let bytes = normalized.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if !bytes[index].is_ascii_digit() {
            index += 1;
            continue;
        }
        let Some(season_end) = consume_alias_digits(bytes, index, 2) else {
            index += 1;
            continue;
        };
        if season_end >= bytes.len() || bytes[season_end] != b'x' {
            index += 1;
            continue;
        }
        let Some(episode_end) = consume_alias_digits(bytes, season_end + 1, 3) else {
            index += 1;
            continue;
        };
        let season_number = normalized[index..season_end].parse::<u32>().ok()?;
        let episode_number = normalized[season_end + 1..episode_end]
            .parse::<u32>()
            .ok()?;
        return Some((season_number, episode_number));
    }
    None
}

fn normalize_alias_token(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("")
}

fn parse_prefixed_alias_number(
    normalized: &str,
    prefixes: &[&str],
    max_digits: usize,
) -> Option<u32> {
    for prefix in prefixes {
        let Some(remainder) = normalized.strip_prefix(prefix) else {
            continue;
        };
        let digit_end = consume_alias_digits(remainder.as_bytes(), 0, max_digits)?;
        return remainder[..digit_end].parse::<u32>().ok();
    }
    None
}

fn consume_alias_digits(bytes: &[u8], start: usize, max_digits: usize) -> Option<usize> {
    if start >= bytes.len() || !bytes[start].is_ascii_digit() {
        return None;
    }
    let mut end = start;
    while end < bytes.len() && bytes[end].is_ascii_digit() && end - start < max_digits {
        end += 1;
    }
    Some(end)
}

#[cfg(target_os = "linux")]
fn file_type_from_node_kind(kind: MountNodeKind) -> fuse3::FileType {
    match kind {
        MountNodeKind::Directory => fuse3::FileType::Directory,
        MountNodeKind::File => fuse3::FileType::RegularFile,
    }
}

#[cfg(target_os = "linux")]
fn prepare_mountpoint(path: &Path, _adapter: ResolvedMountAdapterKind) -> Result<()> {
    std::fs::create_dir_all(path)?;
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_dir() {
        bail!("mountpoint {} is not a directory", path.display());
    }
    Ok(())
}

#[cfg(target_os = "windows")]
fn prepare_mountpoint(path: &Path, adapter: ResolvedMountAdapterKind) -> Result<()> {
    match adapter {
        ResolvedMountAdapterKind::Projfs => {
            std::fs::create_dir_all(path)?;
            let metadata = std::fs::metadata(path)?;
            if !metadata.is_dir() {
                bail!("mountpoint {} is not a directory", path.display());
            }
        }
        ResolvedMountAdapterKind::Winfsp => {
            if is_windows_drive_mountpoint(path) {
                return Ok(());
            }

            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }

            if path.exists() {
                std::fs::remove_dir(path).map_err(|error| {
                    anyhow::anyhow!(
                        "winfsp mountpoint {} must not exist before startup; remove the placeholder directory or choose a different path. Removal failed: {error}",
                        path.display()
                    )
                })?;
            }
        }
        ResolvedMountAdapterKind::Fuse => {
            bail!("the Linux fuse adapter is not supported on Windows hosts");
        }
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn is_windows_drive_mountpoint(path: &Path) -> bool {
    let raw = path.as_os_str().to_string_lossy();
    let normalized = raw.trim_end_matches(['\\', '/']);
    normalized.len() == 2
        && normalized.as_bytes()[1] == b':'
        && normalized.as_bytes()[0].is_ascii_alphabetic()
}

#[cfg(test)]
mod tests {
    use super::{
        build_chunk_read_request, parse_media_semantic_path, InlineRefreshFlight, MountReadRequest,
    };
    use tokio::time::{timeout, Duration};

    fn sample_request(provider_file_id: Option<&str>) -> MountReadRequest {
        MountReadRequest {
            handle_id: 1,
            handle_key: "session:1".to_owned(),
            inode: 42,
            entry_id: "entry-123".to_owned(),
            item_id: "item-123".to_owned(),
            item_external_ref: Some("tmdb:123".to_owned()),
            path: "/movies/Test/Test.mkv".to_owned(),
            semantic_path: parse_media_semantic_path("/movies/Test/Test.mkv", Some("tmdb:123")),
            unrestricted_url: "https://example.invalid/file".to_owned(),
            provider_file_id: provider_file_id.map(str::to_owned),
            offset: 0,
            length: 4096,
            size_bytes: Some(8192),
            remote_direct: true,
        }
    }

    #[test]
    fn chunk_request_uses_entry_id_when_provider_file_id_is_blank() {
        let request = sample_request(Some("   "));
        let chunk_request = build_chunk_read_request(&request);
        assert_eq!(chunk_request.file_id, "entry-123");
        assert!(chunk_request.provider_file_id.is_none());
    }

    #[test]
    fn chunk_request_preserves_provider_file_id_but_uses_entry_id_for_cache_identity() {
        let request = sample_request(Some("provider-abc"));
        let chunk_request = build_chunk_read_request(&request);
        assert_eq!(chunk_request.file_id, "entry-123");
        assert_eq!(
            chunk_request.provider_file_id.as_deref(),
            Some("provider-abc")
        );
    }

    #[test]
    fn semantic_path_is_carried_on_sample_read_request() {
        let request = sample_request(Some("provider-abc"));
        assert_eq!(
            request.semantic_path.path_type.map(|value| value.as_str()),
            Some("movie-file")
        );
        assert_eq!(request.semantic_path.tmdb_id.as_deref(), Some("123"));
    }

    #[tokio::test]
    async fn inline_refresh_wait_returns_finished_value() {
        let flight = InlineRefreshFlight::new();
        flight.finish(Some("https://example.invalid/refreshed".to_owned()));

        let result = timeout(Duration::from_secs(1), flight.wait())
            .await
            .expect("wait should complete");

        assert_eq!(result.as_deref(), Some("https://example.invalid/refreshed"));
    }

    #[tokio::test]
    async fn inline_refresh_finish_wakes_multiple_waiters() {
        let flight = std::sync::Arc::new(InlineRefreshFlight::new());
        let waiter_a = {
            let flight = flight.clone();
            tokio::spawn(async move {
                timeout(Duration::from_secs(1), flight.wait())
                    .await
                    .expect("first waiter should complete")
            })
        };
        let waiter_b = {
            let flight = flight.clone();
            tokio::spawn(async move {
                timeout(Duration::from_secs(1), flight.wait())
                    .await
                    .expect("second waiter should complete")
            })
        };

        tokio::task::yield_now().await;
        flight.finish(Some("https://example.invalid/refreshed".to_owned()));

        let result_a = waiter_a.await.expect("first waiter task should join");
        let result_b = waiter_b.await.expect("second waiter task should join");

        assert_eq!(
            result_a.as_deref(),
            Some("https://example.invalid/refreshed")
        );
        assert_eq!(
            result_b.as_deref(),
            Some("https://example.invalid/refreshed")
        );
    }
}
