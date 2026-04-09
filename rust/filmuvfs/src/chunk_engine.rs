use std::{cmp, sync::Arc};

use bytes::{Bytes, BytesMut};
use dashmap::{mapref::entry::Entry, DashMap};
use thiserror::Error;
use tokio::sync::Notify;

use crate::{
    cache::{CacheEngine, CacheEngineSnapshot},
    chunk_planner::{ChunkPlanner, ChunkPlannerConfig, PlannedChunk, PlannedRead, ReadPattern},
    prefetch::{PrefetchScheduleError, PrefetchScheduler},
    telemetry::{record_chunk_cache_event, record_chunk_read_pattern, record_prefetch_event},
    upstream::{RangeRequest, UpstreamReadError, UpstreamReader},
};

#[derive(Debug, Clone)]
pub struct ChunkEngineConfig {
    pub planner: ChunkPlannerConfig,
    pub prefetch_concurrency: usize,
}

impl Default for ChunkEngineConfig {
    fn default() -> Self {
        Self {
            planner: ChunkPlannerConfig::default(),
            prefetch_concurrency: 4,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkReadRequest {
    pub handle_key: String,
    pub file_id: String,
    pub url: String,
    pub provider_file_id: Option<String>,
    pub offset: u64,
    pub length: u32,
    pub file_size: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct HandleReadState {
    previous_end_exclusive: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkCacheSnapshot {
    pub backend: &'static str,
    pub weighted_size_bytes: u64,
    pub memory_bytes: u64,
    pub memory_max_bytes: u64,
    pub memory_hits: u64,
    pub memory_misses: u64,
    pub disk_bytes: u64,
    pub disk_max_bytes: u64,
    pub disk_hits: u64,
    pub disk_misses: u64,
    pub disk_writes: u64,
    pub disk_write_errors: u64,
    pub disk_evictions: u64,
}

#[derive(Debug, Error)]
pub enum ChunkEngineError {
    #[error("invalid chunk-engine request: {message}")]
    InvalidRequest { message: String },
    #[error("upstream fetch failed: {0}")]
    Upstream(#[from] UpstreamReadError),
    #[error("foreground scheduling failed: {0}")]
    Scheduler(#[from] PrefetchScheduleError),
    #[error(
        "chunk payload length mismatch for offset {offset}: expected {expected} bytes, got {actual}"
    )]
    InvalidChunkPayload {
        offset: u64,
        expected: u64,
        actual: usize,
    },
    #[error("chunk list does not fully cover the requested byte range")]
    IncompleteCoverage,
}

impl ChunkEngineError {
    #[must_use]
    pub fn is_stale(&self) -> bool {
        matches!(self, Self::Upstream(error) if error.is_stale())
    }
}

#[derive(Clone)]
pub struct ChunkEngine {
    planner: ChunkPlanner,
    pub(crate) cache: Arc<dyn CacheEngine>,
    scheduler: PrefetchScheduler,
    upstream_reader: UpstreamReader,
    handle_reads: Arc<DashMap<String, HandleReadState>>,
    in_flight_chunks: Arc<DashMap<String, Arc<Notify>>>,
}

impl std::fmt::Debug for ChunkEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChunkEngine")
            .field("cached_bytes", &self.cache.size_bytes())
            .finish_non_exhaustive()
    }
}

impl ChunkEngine {
    pub fn new(
        cache: Arc<dyn CacheEngine>,
        config: ChunkEngineConfig,
        upstream_reader: UpstreamReader,
    ) -> Result<Self, ChunkEngineError> {
        Ok(Self {
            planner: ChunkPlanner::new(config.planner),
            cache,
            scheduler: PrefetchScheduler::new(config.prefetch_concurrency)?,
            upstream_reader,
            handle_reads: Arc::new(DashMap::new()),
            in_flight_chunks: Arc::new(DashMap::new()),
        })
    }

    #[must_use]
    pub fn preview_read(&self, request: &ChunkReadRequest) -> PlannedRead {
        let previous_end_exclusive = self
            .handle_reads
            .get(&request.handle_key)
            .and_then(|state| state.previous_end_exclusive);
        self.planner.plan_read(
            request.offset,
            request.length,
            request.file_size,
            previous_end_exclusive,
        )
    }

    pub fn register_handle(&self, handle_key: &str) {
        self.scheduler.register_handle(handle_key);
        self.handle_reads.entry(handle_key.to_owned()).or_default();
    }

    pub fn release_handle(&self, handle_key: &str) {
        self.scheduler.cancel_handle(handle_key);
        self.handle_reads.remove(handle_key);
    }

    #[must_use]
    pub fn cache_snapshot(&self) -> ChunkCacheSnapshot {
        let cache_snapshot: CacheEngineSnapshot = self.cache.snapshot();
        ChunkCacheSnapshot {
            backend: cache_snapshot.backend,
            weighted_size_bytes: cache_snapshot.weighted_size_bytes,
            memory_bytes: cache_snapshot.memory_bytes,
            memory_max_bytes: cache_snapshot.memory_max_bytes,
            memory_hits: cache_snapshot.memory_hits,
            memory_misses: cache_snapshot.memory_misses,
            disk_bytes: cache_snapshot.disk_bytes,
            disk_max_bytes: cache_snapshot.disk_max_bytes,
            disk_hits: cache_snapshot.disk_hits,
            disk_misses: cache_snapshot.disk_misses,
            disk_writes: cache_snapshot.disk_writes,
            disk_write_errors: cache_snapshot.disk_write_errors,
            disk_evictions: cache_snapshot.disk_evictions,
        }
    }

    pub async fn read(&self, request: ChunkReadRequest) -> Result<Bytes, ChunkEngineError> {
        if request.length == 0 {
            return Ok(Bytes::new());
        }
        if request.length > 64 * 1024 * 1024 {
            tracing::error!(
                handle_key = %request.handle_key,
                file_id = %request.file_id,
                offset = request.offset,
                length = request.length,
                file_size = ?request.file_size,
                "chunk_engine.read rejected oversized request"
            );
            return Err(ChunkEngineError::InvalidRequest {
                message: format!("read length {} exceeds sanity limit", request.length),
            });
        }

        self.register_handle(&request.handle_key);
        let previous_end_exclusive = self
            .handle_reads
            .get(&request.handle_key)
            .and_then(|state| state.previous_end_exclusive);

        let mut planned = self.planner.plan_read(
            request.offset,
            request.length,
            request.file_size,
            previous_end_exclusive,
        );
        if planned.chunks.is_empty() {
            return Ok(Bytes::new());
        }

        if self
            .all_chunks_cached(&request.file_id, &planned.chunks)
            .await
        {
            planned.pattern = ReadPattern::CacheHit;
            planned.prefetch_chunks.clear();
        }
        record_chunk_read_pattern(read_pattern_name(planned.pattern));
        tracing::info!(
            handle_key = %request.handle_key,
            file_id = %request.file_id,
            offset = request.offset,
            length = request.length,
            file_size = ?request.file_size,
            pattern = read_pattern_name(planned.pattern),
            chunk_count = planned.chunks.len(),
            prefetch_chunk_count = planned.prefetch_chunks.len(),
            previous_end_exclusive = ?previous_end_exclusive,
            request_end_exclusive = planned.request_end_exclusive,
            "chunk_engine.read plan"
        );
        if let Some(chunk) = planned
            .chunks
            .iter()
            .find(|chunk| chunk.length == 0 || chunk.length > 64 * 1024 * 1024)
        {
            tracing::error!(
                handle_key = %request.handle_key,
                file_id = %request.file_id,
                offset = request.offset,
                length = request.length,
                chunk_offset = chunk.offset,
                chunk_length = chunk.length,
                "chunk_engine.read rejected absurd chunk"
            );
            return Err(ChunkEngineError::InvalidRequest {
                message: format!(
                    "planned chunk length {} at offset {} rejected",
                    chunk.length, chunk.offset
                ),
            });
        }

        let mut stitched = BytesMut::with_capacity(request.length as usize);
        let mut covered_until = request.offset;
        for chunk in &planned.chunks {
            let payload = self.load_chunk_foreground(&request, *chunk).await?;
            let overlap_start = cmp::max(request.offset, chunk.offset);
            let overlap_end = cmp::min(
                planned.request_end_exclusive,
                chunk.offset.saturating_add(chunk.length),
            );
            if overlap_start >= overlap_end {
                continue;
            }
            if overlap_start > covered_until {
                return Err(ChunkEngineError::IncompleteCoverage);
            }

            let payload_start = usize::try_from(overlap_start.saturating_sub(chunk.offset))
                .map_err(|_| ChunkEngineError::InvalidRequest {
                    message: "payload start offset overflowed usize".to_owned(),
                })?;
            let payload_end =
                usize::try_from(overlap_end.saturating_sub(chunk.offset)).map_err(|_| {
                    ChunkEngineError::InvalidRequest {
                        message: "payload end offset overflowed usize".to_owned(),
                    }
                })?;
            stitched.extend_from_slice(&payload[payload_start..payload_end]);
            covered_until = overlap_end;
        }

        if covered_until < planned.request_end_exclusive {
            return Err(ChunkEngineError::IncompleteCoverage);
        }

        self.handle_reads.insert(
            request.handle_key.clone(),
            HandleReadState {
                previous_end_exclusive: Some(planned.request_end_exclusive),
            },
        );

        self.spawn_prefetches(request, planned).await;
        Ok(stitched.freeze())
    }

    pub async fn prefetch_ahead(
        &self,
        request: ChunkReadRequest,
        chunk_count: u32,
    ) -> Result<(), ChunkEngineError> {
        if chunk_count == 0 || request.length == 0 {
            return Ok(());
        }

        let planned = self.preview_read(&request);
        if !planned.pattern.should_prefetch() || planned.chunks.is_empty() {
            return Ok(());
        }

        let chunk_size = planned
            .chunks
            .last()
            .map(|chunk| chunk.length)
            .unwrap_or(u64::from(request.length));
        let mut next_offset = planned
            .chunks
            .last()
            .map(|chunk| chunk.offset.saturating_add(chunk.length))
            .unwrap_or_else(|| request.offset.saturating_add(u64::from(request.length)));

        for _ in 0..chunk_count {
            let chunk = match request.file_size {
                Some(file_size) if next_offset >= file_size => break,
                Some(file_size) => PlannedChunk {
                    offset: next_offset,
                    length: chunk_size.min(file_size.saturating_sub(next_offset)),
                },
                None => PlannedChunk {
                    offset: next_offset,
                    length: chunk_size,
                },
            };

            if chunk.length == 0 {
                break;
            }

            if !self
                .spawn_prefetch_chunk_if_missing(request.clone(), chunk)
                .await
            {
                break;
            }
            next_offset = chunk.offset.saturating_add(chunk.length);
        }

        Ok(())
    }

    pub async fn prefetch_request(
        &self,
        request: ChunkReadRequest,
    ) -> Result<(), ChunkEngineError> {
        if request.length == 0 {
            return Ok(());
        }

        self.register_handle(&request.handle_key);
        let planned = self.preview_read(&request);
        if planned.chunks.is_empty() {
            return Ok(());
        }

        if self
            .all_chunks_cached(&request.file_id, &planned.chunks)
            .await
        {
            record_prefetch_event("request_cache_hit");
            return Ok(());
        }

        for chunk in planned.chunks {
            if !self
                .spawn_prefetch_chunk_if_missing(request.clone(), chunk)
                .await
            {
                record_prefetch_event("request_backpressure");
                break;
            }
        }

        Ok(())
    }

    async fn all_chunks_cached(&self, file_id: &str, chunks: &[PlannedChunk]) -> bool {
        for chunk in chunks {
            if self.cache.get(&cache_key(file_id, *chunk)).await.is_none() {
                record_chunk_cache_event("miss");
                return false;
            }
            record_chunk_cache_event("hit");
        }
        true
    }

    async fn load_chunk_foreground(
        &self,
        request: &ChunkReadRequest,
        chunk: PlannedChunk,
    ) -> Result<Bytes, ChunkEngineError> {
        let key = cache_key(&request.file_id, chunk);
        if let Some(bytes) = self.cache.get(&key).await {
            record_chunk_cache_event("hit");
            tracing::debug!(
                handle_key = %request.handle_key,
                file_id = %request.file_id,
                chunk_offset = chunk.offset,
                chunk_length = chunk.length,
                "chunk_engine.read served chunk from cache"
            );
            return Ok(bytes);
        }
        record_chunk_cache_event("miss");
        tracing::info!(
            handle_key = %request.handle_key,
            file_id = %request.file_id,
            chunk_offset = chunk.offset,
            chunk_length = chunk.length,
            "chunk_engine.read cache miss; fetching foreground chunk"
        );

        loop {
            let fetch_gate = match self.in_flight_chunks.entry(key.clone()) {
                Entry::Vacant(entry) => {
                    let notify = Arc::new(Notify::new());
                    entry.insert(Arc::clone(&notify));
                    Some(notify)
                }
                Entry::Occupied(entry) => {
                    let notify = Arc::clone(entry.get());
                    tracing::debug!(
                        handle_key = %request.handle_key,
                        file_id = %request.file_id,
                        chunk_offset = chunk.offset,
                        chunk_length = chunk.length,
                        "chunk_engine.read waiting for in-flight foreground fetch"
                    );
                    let notified = notify.notified();
                    drop(entry);
                    notified.await;
                    if let Some(bytes) = self.cache.get(&key).await {
                        record_chunk_cache_event("hit_after_inflight_wait");
                        tracing::debug!(
                            handle_key = %request.handle_key,
                            file_id = %request.file_id,
                            chunk_offset = chunk.offset,
                            chunk_length = chunk.length,
                            "chunk_engine.read resolved from cache after in-flight wait"
                        );
                        return Ok(bytes);
                    }
                    record_chunk_cache_event("miss_after_inflight_wait");
                    continue;
                }
            };

            let _permit = self.scheduler.acquire_foreground().await?;
            if let Some(bytes) = self.cache.get(&key).await {
                record_chunk_cache_event("hit_after_wait");
                self.in_flight_chunks.remove(&key);
                if let Some(notify) = fetch_gate {
                    notify.notify_waiters();
                }
                tracing::debug!(
                    handle_key = %request.handle_key,
                    file_id = %request.file_id,
                    chunk_offset = chunk.offset,
                    chunk_length = chunk.length,
                    "chunk_engine.read foreground wait resolved from cache"
                );
                return Ok(bytes);
            }
            record_chunk_cache_event("miss_after_wait");
            tracing::info!(
                handle_key = %request.handle_key,
                file_id = %request.file_id,
                chunk_offset = chunk.offset,
                chunk_length = chunk.length,
                "chunk_engine.read foreground wait still missing; fetching upstream"
            );

            let result = self.fetch_chunk(request, chunk).await;
            let removed = self.in_flight_chunks.remove(&key).map(|(_, notify)| notify);
            match result {
                Ok(bytes) => {
                    self.insert_cache_entry(key.clone(), bytes.clone()).await;
                    if let Some(notify) = removed.or(fetch_gate) {
                        notify.notify_waiters();
                    }
                    return Ok(bytes);
                }
                Err(error) => {
                    if let Some(notify) = removed.or(fetch_gate) {
                        notify.notify_waiters();
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn fetch_chunk(
        &self,
        request: &ChunkReadRequest,
        chunk: PlannedChunk,
    ) -> Result<Bytes, ChunkEngineError> {
        let size = u32::try_from(chunk.length).map_err(|_| ChunkEngineError::InvalidRequest {
            message: format!("chunk length {} exceeds u32::MAX", chunk.length),
        })?;
        let bytes = self
            .upstream_reader
            .fetch_range(RangeRequest::new(request.url.clone(), chunk.offset, size))
            .await?;
        if bytes.len() != chunk.length as usize {
            return Err(ChunkEngineError::InvalidChunkPayload {
                offset: chunk.offset,
                expected: chunk.length,
                actual: bytes.len(),
            });
        }
        Ok(bytes)
    }

    async fn spawn_prefetches(&self, request: ChunkReadRequest, planned: PlannedRead) {
        if !planned.pattern.should_prefetch() || planned.prefetch_chunks.is_empty() {
            record_prefetch_event("skipped_pattern");
            tracing::debug!(
                handle_key = %request.handle_key,
                file_id = %request.file_id,
                pattern = read_pattern_name(planned.pattern),
                prefetch_chunk_count = planned.prefetch_chunks.len(),
                "chunk_engine.prefetch skipped"
            );
            return;
        }

        tracing::info!(
            handle_key = %request.handle_key,
            file_id = %request.file_id,
            pattern = read_pattern_name(planned.pattern),
            prefetch_chunk_count = planned.prefetch_chunks.len(),
            "chunk_engine.prefetch scheduling"
        );

        for chunk in planned.prefetch_chunks {
            if !self
                .spawn_prefetch_chunk_if_missing(request.clone(), chunk)
                .await
            {
                record_prefetch_event("background_backpressure");
                break;
            }
        }
    }

    async fn spawn_prefetch_chunk_if_missing(
        &self,
        request: ChunkReadRequest,
        chunk: PlannedChunk,
    ) -> bool {
        let key = cache_key(&request.file_id, chunk);
        if self.cache.get(&key).await.is_some() {
            record_chunk_cache_event("prefetch_hit");
            record_prefetch_event("skipped_cached");
            return true;
        }

        let engine = self.clone();
        let handle_key = request.handle_key.clone();
        let spawned = self.scheduler.spawn_background(&handle_key, async move {
            if engine.cache.get(&key).await.is_some() {
                record_chunk_cache_event("prefetch_hit_after_spawn");
                return;
            }

            match engine.fetch_chunk(&request, chunk).await {
                Ok(bytes) => {
                    record_prefetch_event("background_populated");
                    engine.insert_cache_entry(key, bytes).await
                }
                Err(error) => {
                    record_prefetch_event("background_error");
                    tracing::debug!(
                        error = %error,
                        handle_key = %request.handle_key,
                        chunk_offset = chunk.offset,
                        chunk_length = chunk.length,
                        "background prefetch did not populate the chunk cache"
                    );
                }
            }
        });
        if spawned {
            record_prefetch_event("background_spawned");
        } else {
            record_prefetch_event("background_backpressure");
        }
        spawned
    }

    async fn insert_cache_entry(&self, key: String, bytes: Bytes) {
        record_chunk_cache_event("insert");
        self.cache.insert(key, bytes).await;
    }
}

fn cache_key(file_id: &str, chunk: PlannedChunk) -> String {
    format!("{file_id}:{}:{}", chunk.offset, chunk.length)
}

fn read_pattern_name(pattern: ReadPattern) -> &'static str {
    match pattern {
        ReadPattern::HeaderScan => "header_scan",
        ReadPattern::SequentialScan => "sequential_scan",
        ReadPattern::RandomAccess => "random_access",
        ReadPattern::TailProbe => "tail_probe",
        ReadPattern::CacheHit => "cache_hit",
    }
}
