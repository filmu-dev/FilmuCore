use std::{
    fs,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter, ObservableGauge},
    trace::TracerProvider as _,
    KeyValue,
};
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::{
    metrics::SdkMeterProvider, propagation::TraceContextPropagator, trace::SdkTracerProvider,
    Resource,
};
use serde::Serialize;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::{
    catalog::state::CatalogStateStore,
    config::SidecarConfig,
    mount::{MountHandleAgePercentiles, MountHandleDepthRollup, MountRuntime},
    SERVICE_NAME,
};

static FILMUVFS_METRICS: OnceLock<FilmuvfsMetrics> = OnceLock::new();

pub fn metrics() -> Option<&'static FilmuvfsMetrics> {
    FILMUVFS_METRICS.get()
}

pub fn record_read_request(result: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_read_request(result);
    }
}

pub fn record_mounted_read_duration(duration: Duration, result: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_mounted_read_duration(duration, result);
    }
}

pub fn record_handle_startup_duration(duration: Duration, result: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_handle_startup_duration(duration, result);
    }
}

pub fn record_upstream_fetch_bytes(bytes: u64) {
    if let Some(metrics) = metrics() {
        metrics.record_upstream_fetch_bytes(bytes);
    }
}

pub fn record_upstream_fetch_duration(duration: Duration) {
    if let Some(metrics) = metrics() {
        metrics.record_upstream_fetch_duration(duration);
    }
}

pub fn record_upstream_failure(result: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_upstream_failure(result);
    }
}

pub fn record_upstream_retryable_event(event: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_upstream_retryable_event(event);
    }
}

pub fn record_backend_fallback(result: &'static str, reason: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_backend_fallback(result, reason);
    }
}

pub fn record_chunk_cache_event(event: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_chunk_cache_event(event);
    }
}

pub fn record_chunk_read_pattern(pattern: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_chunk_read_pattern(pattern);
    }
}

pub fn record_prefetch_event(event: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_prefetch_event(event);
    }
}

pub fn record_inline_refresh(result: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_inline_refresh(result);
    }
}

pub fn record_windows_projfs_callback(result: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_windows_projfs_callback(result);
    }
}

pub fn record_windows_projfs_callback_duration(duration: Duration, result: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_windows_projfs_callback_duration(duration, result);
    }
}

pub fn record_windows_projfs_stream_handle_event(event: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_windows_projfs_stream_handle_event(event);
    }
}

pub fn record_windows_projfs_notification(notification: &'static str) {
    if let Some(metrics) = metrics() {
        metrics.record_windows_projfs_notification(notification);
    }
}

pub fn log_windows_projfs_summary() {
    if let Some(metrics) = metrics() {
        metrics.log_windows_projfs_summary();
    }
}

pub fn write_runtime_status_snapshot(path: &Path, config: &SidecarConfig) -> Result<()> {
    if let Some(metrics) = metrics() {
        metrics.write_runtime_status_snapshot(path, config)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsRuntimeStatusSnapshot {
    pub service_name: String,
    pub service_version: String,
    pub daemon_id: String,
    pub session_id: String,
    pub mountpoint: String,
    pub mount_adapter: String,
    pub grpc_endpoint: String,
    pub generated_at_unix_seconds: u64,
    pub catalog: FilmuvfsCatalogStatusSnapshot,
    pub runtime: FilmuvfsRuntimeGaugeSnapshot,
    pub handle_startup: FilmuvfsHandleStartupStatusSnapshot,
    pub mounted_reads: FilmuvfsMountedReadStatusSnapshot,
    pub upstream_fetch: FilmuvfsUpstreamFetchStatusSnapshot,
    pub upstream_failures: FilmuvfsUpstreamFailureStatusSnapshot,
    pub upstream_retryable_events: FilmuvfsUpstreamRetryableStatusSnapshot,
    pub backend_fallback: FilmuvfsBackendFallbackStatusSnapshot,
    pub chunk_cache: FilmuvfsChunkCacheStatusSnapshot,
    pub chunk_read_patterns: FilmuvfsChunkReadPatternSnapshot,
    pub prefetch: FilmuvfsPrefetchStatusSnapshot,
    pub chunk_coalescing: FilmuvfsChunkCoalescingStatusSnapshot,
    pub inline_refresh: FilmuvfsInlineRefreshStatusSnapshot,
    pub windows_projfs: FilmuvfsWindowsProjfsStatusSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsCatalogStatusSnapshot {
    pub directories: u64,
    pub files: u64,
    pub total_entries: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsRuntimeGaugeSnapshot {
    pub open_handles: u64,
    pub peak_open_handles: u64,
    pub active_reads: u64,
    pub peak_active_reads: u64,
    pub chunk_cache_weighted_bytes: u64,
    pub active_handle_summaries: Vec<String>,
    pub active_handle_age_percentiles_ms: MountHandleAgePercentiles,
    pub handle_depth_rollups: Vec<MountHandleDepthRollup>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsHandleStartupStatusSnapshot {
    pub total: u64,
    pub ok: u64,
    pub error: u64,
    pub estale: u64,
    pub cancelled: u64,
    pub average_duration_ms: f64,
    pub max_duration_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsMountedReadStatusSnapshot {
    pub total: u64,
    pub ok: u64,
    pub error: u64,
    pub estale: u64,
    pub cancelled: u64,
    pub average_duration_ms: f64,
    pub max_duration_ms: f64,
    pub duration_buckets: Vec<FilmuvfsDurationBucketSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsDurationBucketSnapshot {
    pub label: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsUpstreamFetchStatusSnapshot {
    pub operations: u64,
    pub bytes_total: u64,
    pub average_duration_ms: f64,
    pub max_duration_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsUpstreamFailureStatusSnapshot {
    pub invalid_url: u64,
    pub build_request: u64,
    pub network: u64,
    pub stale_status: u64,
    pub unexpected_status: u64,
    pub unexpected_status_too_many_requests: u64,
    pub unexpected_status_server_error: u64,
    pub read_body: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsUpstreamRetryableStatusSnapshot {
    pub network: u64,
    pub read_body: u64,
    pub status_too_many_requests: u64,
    pub status_server_error: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsBackendFallbackStatusSnapshot {
    pub attempts: u64,
    pub success: u64,
    pub failure: u64,
    pub attempts_direct_read_failure: u64,
    pub attempts_inline_refresh_unavailable: u64,
    pub attempts_post_inline_refresh_failure: u64,
    pub success_direct_read_failure: u64,
    pub success_inline_refresh_unavailable: u64,
    pub success_post_inline_refresh_failure: u64,
    pub failure_direct_read_failure: u64,
    pub failure_inline_refresh_unavailable: u64,
    pub failure_post_inline_refresh_failure: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsChunkCacheStatusSnapshot {
    pub backend: &'static str,
    pub total_events: u64,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub prefetch_hits: u64,
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

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsChunkReadPatternSnapshot {
    pub header_scan: u64,
    pub sequential_scan: u64,
    pub random_access: u64,
    pub tail_probe: u64,
    pub cache_hit: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsPrefetchStatusSnapshot {
    pub request_cache_hit: u64,
    pub background_spawned: u64,
    pub background_populated: u64,
    pub background_backpressure: u64,
    pub fairness_denied: u64,
    pub global_backpressure_denied: u64,
    pub background_error: u64,
    pub skipped_pattern: u64,
    pub skipped_cached: u64,
    pub adaptive_scheduled: u64,
    pub adaptive_error: u64,
    pub startup_scheduled: u64,
    pub startup_error: u64,
    pub concurrency_limit: u64,
    pub max_background_per_handle: u64,
    pub available_permits: u64,
    pub active_permits: u64,
    pub active_background_tasks: u64,
    pub peak_active_background_tasks: u64,
    pub handles_with_background_tasks: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsChunkCoalescingStatusSnapshot {
    pub in_flight_chunks: u64,
    pub peak_in_flight_chunks: u64,
    pub waits_total: u64,
    pub waits_hit: u64,
    pub waits_miss: u64,
    pub wait_average_duration_ms: f64,
    pub wait_max_duration_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsInlineRefreshStatusSnapshot {
    pub success: u64,
    pub no_url: u64,
    pub error: u64,
    pub timeout: u64,
    pub skipped_missing_provider_file_id: u64,
    pub reused_catalog_url: u64,
    pub dedup_wait: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilmuvfsWindowsProjfsStatusSnapshot {
    pub callbacks_ok: u64,
    pub callbacks_error: u64,
    pub callbacks_estale: u64,
    pub callbacks_cancelled: u64,
    pub callback_count: u64,
    pub callback_average_ms: f64,
    pub callback_max_ms: f64,
    pub stream_handles_opened: u64,
    pub stream_handles_reused: u64,
    pub stream_handles_reused_race: u64,
    pub stream_handles_released: u64,
    pub stream_handles_released_on_shutdown: u64,
    pub notifications_closed_clean: u64,
    pub notifications_closed_modified: u64,
    pub notifications_closed_deleted: u64,
    pub notifications_other: u64,
}

fn update_max(target: &AtomicU64, candidate: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        (candidate > current).then_some(candidate)
    });
}

fn atomic_average_millis(total_micros: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        (total_micros as f64 / count as f64) / 1000.0
    }
}

fn atomic_max_millis(max_micros: u64) -> f64 {
    max_micros as f64 / 1000.0
}

fn atomic_write_json(path: &Path, payload: &FilmuvfsRuntimeStatusSnapshot) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let temp_path = path.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(payload)?;
    fs::write(&temp_path, body)?;
    let _ = fs::remove_file(path);
    fs::rename(temp_path, path)?;
    Ok(())
}

pub struct FilmuvfsMetrics {
    catalog_state: Arc<CatalogStateStore>,
    mount_runtime: Arc<MountRuntime>,
    read_requests_total: Counter<u64>,
    mounted_read_duration_seconds: Histogram<f64>,
    handle_startup_duration_seconds: Histogram<f64>,
    upstream_fetch_bytes_total: Counter<u64>,
    upstream_fetch_duration_seconds: Histogram<f64>,
    upstream_failures_total: Counter<u64>,
    upstream_retryable_events_total: Counter<u64>,
    backend_fallback_total: Counter<u64>,
    chunk_cache_events_total: Counter<u64>,
    chunk_read_patterns_total: Counter<u64>,
    prefetch_events_total: Counter<u64>,
    inline_refresh_total: Counter<u64>,
    windows_projfs_callbacks_total: Counter<u64>,
    windows_projfs_callback_duration_seconds: Histogram<f64>,
    windows_projfs_stream_handle_events_total: Counter<u64>,
    windows_projfs_notifications_total: Counter<u64>,
    read_requests_ok: AtomicU64,
    read_requests_error: AtomicU64,
    read_requests_estale: AtomicU64,
    read_requests_cancelled: AtomicU64,
    mounted_read_duration_count: AtomicU64,
    mounted_read_duration_micros_total: AtomicU64,
    mounted_read_duration_micros_max: AtomicU64,
    mounted_read_duration_bucket_le_5ms: AtomicU64,
    mounted_read_duration_bucket_le_25ms: AtomicU64,
    mounted_read_duration_bucket_le_100ms: AtomicU64,
    mounted_read_duration_bucket_le_250ms: AtomicU64,
    mounted_read_duration_bucket_gt_250ms: AtomicU64,
    handle_startup_ok: AtomicU64,
    handle_startup_error: AtomicU64,
    handle_startup_estale: AtomicU64,
    handle_startup_cancelled: AtomicU64,
    handle_startup_duration_count: AtomicU64,
    handle_startup_duration_micros_total: AtomicU64,
    handle_startup_duration_micros_max: AtomicU64,
    upstream_fetch_operations_total: AtomicU64,
    upstream_fetch_bytes_total_atomic: AtomicU64,
    upstream_fetch_duration_count: AtomicU64,
    upstream_fetch_duration_micros_total: AtomicU64,
    upstream_fetch_duration_micros_max: AtomicU64,
    upstream_fail_invalid_url: AtomicU64,
    upstream_fail_build_request: AtomicU64,
    upstream_fail_network: AtomicU64,
    upstream_fail_stale_status: AtomicU64,
    upstream_fail_unexpected_status: AtomicU64,
    upstream_fail_unexpected_status_too_many_requests: AtomicU64,
    upstream_fail_unexpected_status_server_error: AtomicU64,
    upstream_fail_read_body: AtomicU64,
    upstream_retryable_network: AtomicU64,
    upstream_retryable_read_body: AtomicU64,
    upstream_retryable_status_too_many_requests: AtomicU64,
    upstream_retryable_status_server_error: AtomicU64,
    backend_fallback_attempts: AtomicU64,
    backend_fallback_success: AtomicU64,
    backend_fallback_failure: AtomicU64,
    backend_fallback_attempts_direct_read_failure: AtomicU64,
    backend_fallback_attempts_inline_refresh_unavailable: AtomicU64,
    backend_fallback_attempts_post_inline_refresh_failure: AtomicU64,
    backend_fallback_success_direct_read_failure: AtomicU64,
    backend_fallback_success_inline_refresh_unavailable: AtomicU64,
    backend_fallback_success_post_inline_refresh_failure: AtomicU64,
    backend_fallback_failure_direct_read_failure: AtomicU64,
    backend_fallback_failure_inline_refresh_unavailable: AtomicU64,
    backend_fallback_failure_post_inline_refresh_failure: AtomicU64,
    chunk_cache_hits: AtomicU64,
    chunk_cache_misses: AtomicU64,
    chunk_cache_inserts: AtomicU64,
    chunk_cache_prefetch_hits: AtomicU64,
    read_pattern_header_scan: AtomicU64,
    read_pattern_sequential_scan: AtomicU64,
    read_pattern_random_access: AtomicU64,
    read_pattern_tail_probe: AtomicU64,
    read_pattern_cache_hit: AtomicU64,
    prefetch_request_cache_hit: AtomicU64,
    prefetch_background_spawned: AtomicU64,
    prefetch_background_populated: AtomicU64,
    prefetch_background_backpressure: AtomicU64,
    prefetch_background_error: AtomicU64,
    prefetch_skipped_pattern: AtomicU64,
    prefetch_skipped_cached: AtomicU64,
    prefetch_adaptive_scheduled: AtomicU64,
    prefetch_adaptive_error: AtomicU64,
    prefetch_startup_scheduled: AtomicU64,
    prefetch_startup_error: AtomicU64,
    inline_refresh_success: AtomicU64,
    inline_refresh_no_url: AtomicU64,
    inline_refresh_error: AtomicU64,
    inline_refresh_timeout: AtomicU64,
    inline_refresh_skipped_missing_provider_file_id: AtomicU64,
    inline_refresh_reused_catalog_url: AtomicU64,
    inline_refresh_dedup_wait: AtomicU64,
    windows_projfs_callbacks_ok: AtomicU64,
    windows_projfs_callbacks_error: AtomicU64,
    windows_projfs_callbacks_estale: AtomicU64,
    windows_projfs_callbacks_cancelled: AtomicU64,
    windows_projfs_callback_duration_count: AtomicU64,
    windows_projfs_callback_duration_micros_total: AtomicU64,
    windows_projfs_callback_duration_micros_max: AtomicU64,
    windows_projfs_stream_handle_opened: AtomicU64,
    windows_projfs_stream_handle_reused: AtomicU64,
    windows_projfs_stream_handle_reused_race: AtomicU64,
    windows_projfs_stream_handle_released: AtomicU64,
    windows_projfs_stream_handle_released_on_shutdown: AtomicU64,
    windows_projfs_notification_closed_clean: AtomicU64,
    windows_projfs_notification_closed_modified: AtomicU64,
    windows_projfs_notification_closed_deleted: AtomicU64,
    windows_projfs_notification_other: AtomicU64,
    _catalog_entries_total: ObservableGauge<u64>,
    _chunk_cache_weighted_bytes: ObservableGauge<u64>,
    _open_handles_total: ObservableGauge<u64>,
}

impl FilmuvfsMetrics {
    fn new(
        meter: Meter,
        catalog_state: Arc<CatalogStateStore>,
        mount_runtime: Arc<MountRuntime>,
    ) -> Self {
        let read_requests_total = meter
            .u64_counter("filmuvfs_read_requests_total")
            .with_description("Total mounted read requests served by filmuvfs")
            .build();
        let mounted_read_duration_seconds = meter
            .f64_histogram("filmuvfs_mounted_read_duration_seconds")
            .with_description("End-to-end mounted read duration in seconds")
            .with_unit("s")
            .build();
        let handle_startup_duration_seconds = meter
            .f64_histogram("filmuvfs_handle_startup_duration_seconds")
            .with_description("Latency from mounted handle open to first completed read in seconds")
            .with_unit("s")
            .build();
        let upstream_fetch_bytes_total = meter
            .u64_counter("filmuvfs_upstream_fetch_bytes_total")
            .with_description("Total upstream bytes fetched directly from provider URLs")
            .with_unit("By")
            .build();
        let upstream_fetch_duration_seconds = meter
            .f64_histogram("filmuvfs_upstream_fetch_duration_seconds")
            .with_description("Duration of upstream range fetches in seconds")
            .with_unit("s")
            .build();
        let upstream_failures_total = meter
            .u64_counter("filmuvfs_upstream_failures_total")
            .with_description("Classified upstream read failures observed by the mounted runtime")
            .build();
        let upstream_retryable_events_total = meter
            .u64_counter("filmuvfs_upstream_retryable_events_total")
            .with_description("Retryable upstream pressure and transport events seen before eventual success or failure")
            .build();
        let backend_fallback_total = meter
            .u64_counter("filmuvfs_backend_fallback_total")
            .with_description(
                "Backend HTTP fallback attempts and outcomes observed by the mounted runtime",
            )
            .build();
        let chunk_cache_events_total = meter
            .u64_counter("filmuvfs_chunk_cache_events_total")
            .with_description("Chunk cache lookups, inserts, and prefetch cache outcomes")
            .build();
        let chunk_read_patterns_total = meter
            .u64_counter("filmuvfs_chunk_read_patterns_total")
            .with_description("Chunk planner read pattern classifications")
            .build();
        let prefetch_events_total = meter
            .u64_counter("filmuvfs_prefetch_events_total")
            .with_description("Chunk prefetch scheduling outcomes")
            .build();
        let inline_refresh_total = meter
            .u64_counter("filmuvfs_inline_refresh_total")
            .with_description("Inline stale-link refresh outcomes observed during mounted reads")
            .build();
        let windows_projfs_callbacks_total = meter
            .u64_counter("filmuvfs_windows_projfs_callbacks_total")
            .with_description("Total ProjFS GetFileData callbacks handled by the Windows adapter")
            .build();
        let windows_projfs_callback_duration_seconds = meter
            .f64_histogram("filmuvfs_windows_projfs_callback_duration_seconds")
            .with_description("Duration of ProjFS GetFileData callbacks in seconds")
            .with_unit("s")
            .build();
        let windows_projfs_stream_handle_events_total = meter
            .u64_counter("filmuvfs_windows_projfs_stream_handle_events_total")
            .with_description("Total Windows ProjFS per-stream handle lifecycle events")
            .build();
        let windows_projfs_notifications_total = meter
            .u64_counter("filmuvfs_windows_projfs_notifications_total")
            .with_description("Total Windows ProjFS notifications observed by the native adapter")
            .build();

        let catalog_entries_state = Arc::clone(&catalog_state);
        let catalog_entries_total = meter
            .u64_observable_gauge("filmuvfs_catalog_entries_total")
            .with_description("Current number of catalog entries loaded into the sidecar")
            .with_callback(move |observer| {
                let counts = catalog_entries_state.counts();
                observer.observe((counts.directories + counts.files) as u64, &[]);
            })
            .build();

        let open_handles_runtime = Arc::clone(&mount_runtime);
        let open_handles_total = meter
            .u64_observable_gauge("filmuvfs_open_handles_total")
            .with_description("Current number of open mounted file handles")
            .with_callback(move |observer| {
                observer.observe(open_handles_runtime.open_handle_count() as u64, &[]);
            })
            .build();
        let chunk_cache_runtime = Arc::clone(&mount_runtime);
        let chunk_cache_weighted_bytes = meter
            .u64_observable_gauge("filmuvfs_chunk_cache_weighted_bytes")
            .with_description("Current weighted size of the Rust chunk cache")
            .with_unit("By")
            .with_callback(move |observer| {
                observer.observe(chunk_cache_runtime.chunk_cache_weighted_size_bytes(), &[]);
            })
            .build();

        Self {
            catalog_state,
            mount_runtime,
            read_requests_total,
            mounted_read_duration_seconds,
            handle_startup_duration_seconds,
            upstream_fetch_bytes_total,
            upstream_fetch_duration_seconds,
            upstream_failures_total,
            upstream_retryable_events_total,
            backend_fallback_total,
            chunk_cache_events_total,
            chunk_read_patterns_total,
            prefetch_events_total,
            inline_refresh_total,
            windows_projfs_callbacks_total,
            windows_projfs_callback_duration_seconds,
            windows_projfs_stream_handle_events_total,
            windows_projfs_notifications_total,
            read_requests_ok: AtomicU64::new(0),
            read_requests_error: AtomicU64::new(0),
            read_requests_estale: AtomicU64::new(0),
            read_requests_cancelled: AtomicU64::new(0),
            mounted_read_duration_count: AtomicU64::new(0),
            mounted_read_duration_micros_total: AtomicU64::new(0),
            mounted_read_duration_micros_max: AtomicU64::new(0),
            mounted_read_duration_bucket_le_5ms: AtomicU64::new(0),
            mounted_read_duration_bucket_le_25ms: AtomicU64::new(0),
            mounted_read_duration_bucket_le_100ms: AtomicU64::new(0),
            mounted_read_duration_bucket_le_250ms: AtomicU64::new(0),
            mounted_read_duration_bucket_gt_250ms: AtomicU64::new(0),
            handle_startup_ok: AtomicU64::new(0),
            handle_startup_error: AtomicU64::new(0),
            handle_startup_estale: AtomicU64::new(0),
            handle_startup_cancelled: AtomicU64::new(0),
            handle_startup_duration_count: AtomicU64::new(0),
            handle_startup_duration_micros_total: AtomicU64::new(0),
            handle_startup_duration_micros_max: AtomicU64::new(0),
            upstream_fetch_operations_total: AtomicU64::new(0),
            upstream_fetch_bytes_total_atomic: AtomicU64::new(0),
            upstream_fetch_duration_count: AtomicU64::new(0),
            upstream_fetch_duration_micros_total: AtomicU64::new(0),
            upstream_fetch_duration_micros_max: AtomicU64::new(0),
            upstream_fail_invalid_url: AtomicU64::new(0),
            upstream_fail_build_request: AtomicU64::new(0),
            upstream_fail_network: AtomicU64::new(0),
            upstream_fail_stale_status: AtomicU64::new(0),
            upstream_fail_unexpected_status: AtomicU64::new(0),
            upstream_fail_unexpected_status_too_many_requests: AtomicU64::new(0),
            upstream_fail_unexpected_status_server_error: AtomicU64::new(0),
            upstream_fail_read_body: AtomicU64::new(0),
            upstream_retryable_network: AtomicU64::new(0),
            upstream_retryable_read_body: AtomicU64::new(0),
            upstream_retryable_status_too_many_requests: AtomicU64::new(0),
            upstream_retryable_status_server_error: AtomicU64::new(0),
            backend_fallback_attempts: AtomicU64::new(0),
            backend_fallback_success: AtomicU64::new(0),
            backend_fallback_failure: AtomicU64::new(0),
            backend_fallback_attempts_direct_read_failure: AtomicU64::new(0),
            backend_fallback_attempts_inline_refresh_unavailable: AtomicU64::new(0),
            backend_fallback_attempts_post_inline_refresh_failure: AtomicU64::new(0),
            backend_fallback_success_direct_read_failure: AtomicU64::new(0),
            backend_fallback_success_inline_refresh_unavailable: AtomicU64::new(0),
            backend_fallback_success_post_inline_refresh_failure: AtomicU64::new(0),
            backend_fallback_failure_direct_read_failure: AtomicU64::new(0),
            backend_fallback_failure_inline_refresh_unavailable: AtomicU64::new(0),
            backend_fallback_failure_post_inline_refresh_failure: AtomicU64::new(0),
            chunk_cache_hits: AtomicU64::new(0),
            chunk_cache_misses: AtomicU64::new(0),
            chunk_cache_inserts: AtomicU64::new(0),
            chunk_cache_prefetch_hits: AtomicU64::new(0),
            read_pattern_header_scan: AtomicU64::new(0),
            read_pattern_sequential_scan: AtomicU64::new(0),
            read_pattern_random_access: AtomicU64::new(0),
            read_pattern_tail_probe: AtomicU64::new(0),
            read_pattern_cache_hit: AtomicU64::new(0),
            prefetch_request_cache_hit: AtomicU64::new(0),
            prefetch_background_spawned: AtomicU64::new(0),
            prefetch_background_populated: AtomicU64::new(0),
            prefetch_background_backpressure: AtomicU64::new(0),
            prefetch_background_error: AtomicU64::new(0),
            prefetch_skipped_pattern: AtomicU64::new(0),
            prefetch_skipped_cached: AtomicU64::new(0),
            prefetch_adaptive_scheduled: AtomicU64::new(0),
            prefetch_adaptive_error: AtomicU64::new(0),
            prefetch_startup_scheduled: AtomicU64::new(0),
            prefetch_startup_error: AtomicU64::new(0),
            inline_refresh_success: AtomicU64::new(0),
            inline_refresh_no_url: AtomicU64::new(0),
            inline_refresh_error: AtomicU64::new(0),
            inline_refresh_timeout: AtomicU64::new(0),
            inline_refresh_skipped_missing_provider_file_id: AtomicU64::new(0),
            inline_refresh_reused_catalog_url: AtomicU64::new(0),
            inline_refresh_dedup_wait: AtomicU64::new(0),
            windows_projfs_callbacks_ok: AtomicU64::new(0),
            windows_projfs_callbacks_error: AtomicU64::new(0),
            windows_projfs_callbacks_estale: AtomicU64::new(0),
            windows_projfs_callbacks_cancelled: AtomicU64::new(0),
            windows_projfs_callback_duration_count: AtomicU64::new(0),
            windows_projfs_callback_duration_micros_total: AtomicU64::new(0),
            windows_projfs_callback_duration_micros_max: AtomicU64::new(0),
            windows_projfs_stream_handle_opened: AtomicU64::new(0),
            windows_projfs_stream_handle_reused: AtomicU64::new(0),
            windows_projfs_stream_handle_reused_race: AtomicU64::new(0),
            windows_projfs_stream_handle_released: AtomicU64::new(0),
            windows_projfs_stream_handle_released_on_shutdown: AtomicU64::new(0),
            windows_projfs_notification_closed_clean: AtomicU64::new(0),
            windows_projfs_notification_closed_modified: AtomicU64::new(0),
            windows_projfs_notification_closed_deleted: AtomicU64::new(0),
            windows_projfs_notification_other: AtomicU64::new(0),
            _catalog_entries_total: catalog_entries_total,
            _chunk_cache_weighted_bytes: chunk_cache_weighted_bytes,
            _open_handles_total: open_handles_total,
        }
    }

    fn record_read_request(&self, result: &'static str) {
        self.read_requests_total
            .add(1, &[KeyValue::new("result", result)]);
        match result {
            "ok" => {
                self.read_requests_ok.fetch_add(1, Ordering::Relaxed);
            }
            "estale" => {
                self.read_requests_estale.fetch_add(1, Ordering::Relaxed);
            }
            "cancelled" => {
                self.read_requests_cancelled.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.read_requests_error.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_mounted_read_duration(&self, duration: Duration, result: &'static str) {
        self.mounted_read_duration_seconds
            .record(duration.as_secs_f64(), &[KeyValue::new("result", result)]);
        let micros = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.mounted_read_duration_count
            .fetch_add(1, Ordering::Relaxed);
        self.mounted_read_duration_micros_total
            .fetch_add(micros, Ordering::Relaxed);
        update_max(&self.mounted_read_duration_micros_max, micros);
        let millis = duration.as_secs_f64() * 1000.0;
        if millis <= 5.0 {
            self.mounted_read_duration_bucket_le_5ms
                .fetch_add(1, Ordering::Relaxed);
        } else if millis <= 25.0 {
            self.mounted_read_duration_bucket_le_25ms
                .fetch_add(1, Ordering::Relaxed);
        } else if millis <= 100.0 {
            self.mounted_read_duration_bucket_le_100ms
                .fetch_add(1, Ordering::Relaxed);
        } else if millis <= 250.0 {
            self.mounted_read_duration_bucket_le_250ms
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.mounted_read_duration_bucket_gt_250ms
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn mounted_read_duration_buckets_snapshot(&self) -> Vec<FilmuvfsDurationBucketSnapshot> {
        vec![
            FilmuvfsDurationBucketSnapshot {
                label: "le_5_ms".to_owned(),
                count: self
                    .mounted_read_duration_bucket_le_5ms
                    .load(Ordering::Relaxed),
            },
            FilmuvfsDurationBucketSnapshot {
                label: "le_25_ms".to_owned(),
                count: self
                    .mounted_read_duration_bucket_le_25ms
                    .load(Ordering::Relaxed),
            },
            FilmuvfsDurationBucketSnapshot {
                label: "le_100_ms".to_owned(),
                count: self
                    .mounted_read_duration_bucket_le_100ms
                    .load(Ordering::Relaxed),
            },
            FilmuvfsDurationBucketSnapshot {
                label: "le_250_ms".to_owned(),
                count: self
                    .mounted_read_duration_bucket_le_250ms
                    .load(Ordering::Relaxed),
            },
            FilmuvfsDurationBucketSnapshot {
                label: "gt_250_ms".to_owned(),
                count: self
                    .mounted_read_duration_bucket_gt_250ms
                    .load(Ordering::Relaxed),
            },
        ]
    }

    fn record_handle_startup_duration(&self, duration: Duration, result: &'static str) {
        self.handle_startup_duration_seconds
            .record(duration.as_secs_f64(), &[KeyValue::new("result", result)]);
        let micros = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        match result {
            "ok" => {
                self.handle_startup_ok.fetch_add(1, Ordering::Relaxed);
            }
            "estale" => {
                self.handle_startup_estale.fetch_add(1, Ordering::Relaxed);
            }
            "cancelled" => {
                self.handle_startup_cancelled
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.handle_startup_error.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.handle_startup_duration_count
            .fetch_add(1, Ordering::Relaxed);
        self.handle_startup_duration_micros_total
            .fetch_add(micros, Ordering::Relaxed);
        update_max(&self.handle_startup_duration_micros_max, micros);
    }

    fn record_upstream_fetch_bytes(&self, bytes: u64) {
        if bytes > 0 {
            self.upstream_fetch_bytes_total.add(bytes, &[]);
            self.upstream_fetch_bytes_total_atomic
                .fetch_add(bytes, Ordering::Relaxed);
        }
    }

    fn record_upstream_fetch_duration(&self, duration: Duration) {
        self.upstream_fetch_duration_seconds
            .record(duration.as_secs_f64(), &[]);
        let micros = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.upstream_fetch_operations_total
            .fetch_add(1, Ordering::Relaxed);
        self.upstream_fetch_duration_count
            .fetch_add(1, Ordering::Relaxed);
        self.upstream_fetch_duration_micros_total
            .fetch_add(micros, Ordering::Relaxed);
        update_max(&self.upstream_fetch_duration_micros_max, micros);
    }

    fn record_upstream_failure(&self, result: &'static str) {
        self.upstream_failures_total
            .add(1, &[KeyValue::new("result", result)]);
        match result {
            "invalid_url" => {
                self.upstream_fail_invalid_url
                    .fetch_add(1, Ordering::Relaxed);
            }
            "build_request" => {
                self.upstream_fail_build_request
                    .fetch_add(1, Ordering::Relaxed);
            }
            "network" => {
                self.upstream_fail_network.fetch_add(1, Ordering::Relaxed);
            }
            "stale_status" => {
                self.upstream_fail_stale_status
                    .fetch_add(1, Ordering::Relaxed);
            }
            "unexpected_status" => {
                self.upstream_fail_unexpected_status
                    .fetch_add(1, Ordering::Relaxed);
            }
            "unexpected_status_too_many_requests" => {
                self.upstream_fail_unexpected_status_too_many_requests
                    .fetch_add(1, Ordering::Relaxed);
            }
            "unexpected_status_server_error" => {
                self.upstream_fail_unexpected_status_server_error
                    .fetch_add(1, Ordering::Relaxed);
            }
            "read_body" => {
                self.upstream_fail_read_body.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn record_upstream_retryable_event(&self, event: &'static str) {
        self.upstream_retryable_events_total
            .add(1, &[KeyValue::new("event", event)]);
        match event {
            "network" => {
                self.upstream_retryable_network
                    .fetch_add(1, Ordering::Relaxed);
            }
            "read_body" => {
                self.upstream_retryable_read_body
                    .fetch_add(1, Ordering::Relaxed);
            }
            "status_too_many_requests" => {
                self.upstream_retryable_status_too_many_requests
                    .fetch_add(1, Ordering::Relaxed);
            }
            "status_server_error" => {
                self.upstream_retryable_status_server_error
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn record_backend_fallback(&self, result: &'static str, reason: &'static str) {
        self.backend_fallback_total.add(
            1,
            &[
                KeyValue::new("result", result),
                KeyValue::new("reason", reason),
            ],
        );
        match result {
            "attempt" => {
                self.backend_fallback_attempts
                    .fetch_add(1, Ordering::Relaxed);
                match reason {
                    "direct_read_failure" => {
                        self.backend_fallback_attempts_direct_read_failure
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    "inline_refresh_unavailable" => {
                        self.backend_fallback_attempts_inline_refresh_unavailable
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    "post_inline_refresh_failure" => {
                        self.backend_fallback_attempts_post_inline_refresh_failure
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
            "success" => {
                self.backend_fallback_success
                    .fetch_add(1, Ordering::Relaxed);
                match reason {
                    "direct_read_failure" => {
                        self.backend_fallback_success_direct_read_failure
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    "inline_refresh_unavailable" => {
                        self.backend_fallback_success_inline_refresh_unavailable
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    "post_inline_refresh_failure" => {
                        self.backend_fallback_success_post_inline_refresh_failure
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
            "failure" => {
                self.backend_fallback_failure
                    .fetch_add(1, Ordering::Relaxed);
                match reason {
                    "direct_read_failure" => {
                        self.backend_fallback_failure_direct_read_failure
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    "inline_refresh_unavailable" => {
                        self.backend_fallback_failure_inline_refresh_unavailable
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    "post_inline_refresh_failure" => {
                        self.backend_fallback_failure_post_inline_refresh_failure
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    fn record_chunk_cache_event(&self, event: &'static str) {
        self.chunk_cache_events_total
            .add(1, &[KeyValue::new("event", event)]);
        match event {
            "hit" | "hit_after_inflight_wait" | "hit_after_wait" => {
                self.chunk_cache_hits.fetch_add(1, Ordering::Relaxed);
            }
            "miss" | "miss_after_inflight_wait" | "miss_after_wait" => {
                self.chunk_cache_misses.fetch_add(1, Ordering::Relaxed);
            }
            "insert" => {
                self.chunk_cache_inserts.fetch_add(1, Ordering::Relaxed);
            }
            "prefetch_hit" | "prefetch_hit_after_spawn" => {
                self.chunk_cache_prefetch_hits
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn record_chunk_read_pattern(&self, pattern: &'static str) {
        self.chunk_read_patterns_total
            .add(1, &[KeyValue::new("pattern", pattern)]);
        match pattern {
            "header_scan" => {
                self.read_pattern_header_scan
                    .fetch_add(1, Ordering::Relaxed);
            }
            "sequential_scan" => {
                self.read_pattern_sequential_scan
                    .fetch_add(1, Ordering::Relaxed);
            }
            "random_access" => {
                self.read_pattern_random_access
                    .fetch_add(1, Ordering::Relaxed);
            }
            "tail_probe" => {
                self.read_pattern_tail_probe.fetch_add(1, Ordering::Relaxed);
            }
            "cache_hit" => {
                self.read_pattern_cache_hit.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn record_prefetch_event(&self, event: &'static str) {
        self.prefetch_events_total
            .add(1, &[KeyValue::new("event", event)]);
        match event {
            "request_cache_hit" => {
                self.prefetch_request_cache_hit
                    .fetch_add(1, Ordering::Relaxed);
            }
            "background_spawned" => {
                self.prefetch_background_spawned
                    .fetch_add(1, Ordering::Relaxed);
            }
            "background_populated" => {
                self.prefetch_background_populated
                    .fetch_add(1, Ordering::Relaxed);
            }
            "request_backpressure" | "background_backpressure" => {
                self.prefetch_background_backpressure
                    .fetch_add(1, Ordering::Relaxed);
            }
            "background_error" => {
                self.prefetch_background_error
                    .fetch_add(1, Ordering::Relaxed);
            }
            "skipped_pattern" => {
                self.prefetch_skipped_pattern
                    .fetch_add(1, Ordering::Relaxed);
            }
            "skipped_cached" => {
                self.prefetch_skipped_cached.fetch_add(1, Ordering::Relaxed);
            }
            "adaptive_scheduled" => {
                self.prefetch_adaptive_scheduled
                    .fetch_add(1, Ordering::Relaxed);
            }
            "adaptive_error" => {
                self.prefetch_adaptive_error.fetch_add(1, Ordering::Relaxed);
            }
            "startup_scheduled" => {
                self.prefetch_startup_scheduled
                    .fetch_add(1, Ordering::Relaxed);
            }
            "startup_error" => {
                self.prefetch_startup_error.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn record_inline_refresh(&self, result: &'static str) {
        self.inline_refresh_total
            .add(1, &[KeyValue::new("result", result)]);
        match result {
            "success" => {
                self.inline_refresh_success.fetch_add(1, Ordering::Relaxed);
            }
            "no_url" => {
                self.inline_refresh_no_url.fetch_add(1, Ordering::Relaxed);
            }
            "error" => {
                self.inline_refresh_error.fetch_add(1, Ordering::Relaxed);
            }
            "timeout" => {
                self.inline_refresh_timeout.fetch_add(1, Ordering::Relaxed);
            }
            "skipped_missing_provider_file_id" => {
                self.inline_refresh_skipped_missing_provider_file_id
                    .fetch_add(1, Ordering::Relaxed);
            }
            "reused_catalog_url" => {
                self.inline_refresh_reused_catalog_url
                    .fetch_add(1, Ordering::Relaxed);
            }
            "dedup_wait" => {
                self.inline_refresh_dedup_wait
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn record_windows_projfs_callback(&self, result: &'static str) {
        self.windows_projfs_callbacks_total
            .add(1, &[KeyValue::new("result", result)]);
        match result {
            "ok" => {
                self.windows_projfs_callbacks_ok
                    .fetch_add(1, Ordering::Relaxed);
            }
            "estale" => {
                self.windows_projfs_callbacks_estale
                    .fetch_add(1, Ordering::Relaxed);
            }
            "cancelled" => {
                self.windows_projfs_callbacks_cancelled
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.windows_projfs_callbacks_error
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_windows_projfs_callback_duration(&self, duration: Duration, result: &'static str) {
        self.windows_projfs_callback_duration_seconds
            .record(duration.as_secs_f64(), &[KeyValue::new("result", result)]);
        let micros = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.windows_projfs_callback_duration_count
            .fetch_add(1, Ordering::Relaxed);
        self.windows_projfs_callback_duration_micros_total
            .fetch_add(micros, Ordering::Relaxed);
        let _ = self
            .windows_projfs_callback_duration_micros_max
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (micros > current).then_some(micros)
            });
    }

    fn record_windows_projfs_stream_handle_event(&self, event: &'static str) {
        self.windows_projfs_stream_handle_events_total
            .add(1, &[KeyValue::new("event", event)]);
        match event {
            "opened" => {
                self.windows_projfs_stream_handle_opened
                    .fetch_add(1, Ordering::Relaxed);
            }
            "reused" => {
                self.windows_projfs_stream_handle_reused
                    .fetch_add(1, Ordering::Relaxed);
            }
            "reused_race" => {
                self.windows_projfs_stream_handle_reused_race
                    .fetch_add(1, Ordering::Relaxed);
            }
            "released" => {
                self.windows_projfs_stream_handle_released
                    .fetch_add(1, Ordering::Relaxed);
            }
            "released_on_shutdown" => {
                self.windows_projfs_stream_handle_released_on_shutdown
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn record_windows_projfs_notification(&self, notification: &'static str) {
        self.windows_projfs_notifications_total
            .add(1, &[KeyValue::new("notification", notification)]);
        match notification {
            "file_handle_closed_no_modification" => {
                self.windows_projfs_notification_closed_clean
                    .fetch_add(1, Ordering::Relaxed);
            }
            "file_handle_closed_file_modified" => {
                self.windows_projfs_notification_closed_modified
                    .fetch_add(1, Ordering::Relaxed);
            }
            "file_handle_closed_file_deleted" => {
                self.windows_projfs_notification_closed_deleted
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.windows_projfs_notification_other
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn log_windows_projfs_summary(&self) {
        let callback_count = self
            .windows_projfs_callback_duration_count
            .load(Ordering::Relaxed);
        let opened = self
            .windows_projfs_stream_handle_opened
            .load(Ordering::Relaxed);
        let reused = self
            .windows_projfs_stream_handle_reused
            .load(Ordering::Relaxed);
        let reused_race = self
            .windows_projfs_stream_handle_reused_race
            .load(Ordering::Relaxed);
        let notifications = self
            .windows_projfs_notification_closed_clean
            .load(Ordering::Relaxed)
            + self
                .windows_projfs_notification_closed_modified
                .load(Ordering::Relaxed)
            + self
                .windows_projfs_notification_closed_deleted
                .load(Ordering::Relaxed)
            + self
                .windows_projfs_notification_other
                .load(Ordering::Relaxed);
        let callbacks_cancelled = self
            .windows_projfs_callbacks_cancelled
            .load(Ordering::Relaxed);
        if callback_count == 0
            && opened == 0
            && reused == 0
            && notifications == 0
            && callbacks_cancelled == 0
        {
            return;
        }

        let total_micros = self
            .windows_projfs_callback_duration_micros_total
            .load(Ordering::Relaxed);
        let avg_ms = if callback_count == 0 {
            0.0
        } else {
            (total_micros as f64 / callback_count as f64) / 1000.0
        };
        let max_ms = self
            .windows_projfs_callback_duration_micros_max
            .load(Ordering::Relaxed) as f64
            / 1000.0;

        info!(
            callbacks_ok = self.windows_projfs_callbacks_ok.load(Ordering::Relaxed),
            callbacks_estale = self.windows_projfs_callbacks_estale.load(Ordering::Relaxed),
            callbacks_error = self.windows_projfs_callbacks_error.load(Ordering::Relaxed),
            callbacks_cancelled,
            callback_count,
            callback_avg_ms = avg_ms,
            callback_max_ms = max_ms,
            stream_handles_opened = opened,
            stream_handles_reused = reused,
            stream_handles_reused_race = reused_race,
            stream_handles_released = self
                .windows_projfs_stream_handle_released
                .load(Ordering::Relaxed),
            stream_handles_released_on_shutdown = self
                .windows_projfs_stream_handle_released_on_shutdown
                .load(Ordering::Relaxed),
            notifications_closed_clean = self
                .windows_projfs_notification_closed_clean
                .load(Ordering::Relaxed),
            notifications_closed_modified = self
                .windows_projfs_notification_closed_modified
                .load(Ordering::Relaxed),
            notifications_closed_deleted = self
                .windows_projfs_notification_closed_deleted
                .load(Ordering::Relaxed),
            notifications_other = self
                .windows_projfs_notification_other
                .load(Ordering::Relaxed),
            "windows projfs adapter summary"
        );
    }

    fn build_runtime_status_snapshot(
        &self,
        config: &SidecarConfig,
    ) -> FilmuvfsRuntimeStatusSnapshot {
        let chunk_cache_snapshot = self.mount_runtime.chunk_cache_snapshot();
        let prefetch_snapshot = self.mount_runtime.prefetch_snapshot();
        let chunk_coalescing_snapshot = self.mount_runtime.chunk_coalescing_snapshot();
        let catalog_counts = self.catalog_state.counts();
        let read_ok = self.read_requests_ok.load(Ordering::Relaxed);
        let read_error = self.read_requests_error.load(Ordering::Relaxed);
        let read_estale = self.read_requests_estale.load(Ordering::Relaxed);
        let read_cancelled = self.read_requests_cancelled.load(Ordering::Relaxed);
        let read_total = read_ok + read_error + read_estale + read_cancelled;
        let read_duration_count = self.mounted_read_duration_count.load(Ordering::Relaxed);
        let read_duration_total = self
            .mounted_read_duration_micros_total
            .load(Ordering::Relaxed);
        let read_duration_max = self
            .mounted_read_duration_micros_max
            .load(Ordering::Relaxed);
        let handle_startup_ok = self.handle_startup_ok.load(Ordering::Relaxed);
        let handle_startup_error = self.handle_startup_error.load(Ordering::Relaxed);
        let handle_startup_estale = self.handle_startup_estale.load(Ordering::Relaxed);
        let handle_startup_cancelled = self.handle_startup_cancelled.load(Ordering::Relaxed);
        let handle_startup_total = handle_startup_ok
            + handle_startup_error
            + handle_startup_estale
            + handle_startup_cancelled;
        let handle_startup_duration_count =
            self.handle_startup_duration_count.load(Ordering::Relaxed);
        let handle_startup_duration_total = self
            .handle_startup_duration_micros_total
            .load(Ordering::Relaxed);
        let handle_startup_duration_max = self
            .handle_startup_duration_micros_max
            .load(Ordering::Relaxed);
        let upstream_duration_count = self.upstream_fetch_duration_count.load(Ordering::Relaxed);
        let upstream_duration_total = self
            .upstream_fetch_duration_micros_total
            .load(Ordering::Relaxed);
        let upstream_duration_max = self
            .upstream_fetch_duration_micros_max
            .load(Ordering::Relaxed);
        let callback_count = self
            .windows_projfs_callback_duration_count
            .load(Ordering::Relaxed);
        let callback_total = self
            .windows_projfs_callback_duration_micros_total
            .load(Ordering::Relaxed);
        let callback_max = self
            .windows_projfs_callback_duration_micros_max
            .load(Ordering::Relaxed);
        let active_handle_telemetry = self.mount_runtime.active_handle_telemetry(8);

        FilmuvfsRuntimeStatusSnapshot {
            service_name: SERVICE_NAME.to_owned(),
            service_version: env!("CARGO_PKG_VERSION").to_owned(),
            daemon_id: config.daemon_id.clone(),
            session_id: config.session_id.clone(),
            mountpoint: config.mountpoint.display().to_string(),
            mount_adapter: config.mount_adapter.as_str().to_owned(),
            grpc_endpoint: config.grpc_endpoint.clone(),
            generated_at_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            catalog: FilmuvfsCatalogStatusSnapshot {
                directories: catalog_counts.directories as u64,
                files: catalog_counts.files as u64,
                total_entries: (catalog_counts.directories + catalog_counts.files) as u64,
            },
            runtime: FilmuvfsRuntimeGaugeSnapshot {
                open_handles: self.mount_runtime.open_handle_count() as u64,
                peak_open_handles: self.mount_runtime.peak_open_handle_count(),
                active_reads: self.mount_runtime.active_read_count(),
                peak_active_reads: self.mount_runtime.peak_active_read_count(),
                chunk_cache_weighted_bytes: self.mount_runtime.chunk_cache_weighted_size_bytes(),
                active_handle_summaries: self.mount_runtime.active_handle_summaries(10),
                active_handle_age_percentiles_ms: active_handle_telemetry.age_percentiles_ms,
                handle_depth_rollups: active_handle_telemetry.depth_rollups,
            },
            handle_startup: FilmuvfsHandleStartupStatusSnapshot {
                total: handle_startup_total,
                ok: handle_startup_ok,
                error: handle_startup_error,
                estale: handle_startup_estale,
                cancelled: handle_startup_cancelled,
                average_duration_ms: atomic_average_millis(
                    handle_startup_duration_total,
                    handle_startup_duration_count,
                ),
                max_duration_ms: atomic_max_millis(handle_startup_duration_max),
            },
            mounted_reads: FilmuvfsMountedReadStatusSnapshot {
                total: read_total,
                ok: read_ok,
                error: read_error,
                estale: read_estale,
                cancelled: read_cancelled,
                average_duration_ms: atomic_average_millis(
                    read_duration_total,
                    read_duration_count,
                ),
                max_duration_ms: atomic_max_millis(read_duration_max),
                duration_buckets: self.mounted_read_duration_buckets_snapshot(),
            },
            upstream_fetch: FilmuvfsUpstreamFetchStatusSnapshot {
                operations: self.upstream_fetch_operations_total.load(Ordering::Relaxed),
                bytes_total: self
                    .upstream_fetch_bytes_total_atomic
                    .load(Ordering::Relaxed),
                average_duration_ms: atomic_average_millis(
                    upstream_duration_total,
                    upstream_duration_count,
                ),
                max_duration_ms: atomic_max_millis(upstream_duration_max),
            },
            upstream_failures: FilmuvfsUpstreamFailureStatusSnapshot {
                invalid_url: self.upstream_fail_invalid_url.load(Ordering::Relaxed),
                build_request: self.upstream_fail_build_request.load(Ordering::Relaxed),
                network: self.upstream_fail_network.load(Ordering::Relaxed),
                stale_status: self.upstream_fail_stale_status.load(Ordering::Relaxed),
                unexpected_status: self.upstream_fail_unexpected_status.load(Ordering::Relaxed),
                unexpected_status_too_many_requests: self
                    .upstream_fail_unexpected_status_too_many_requests
                    .load(Ordering::Relaxed),
                unexpected_status_server_error: self
                    .upstream_fail_unexpected_status_server_error
                    .load(Ordering::Relaxed),
                read_body: self.upstream_fail_read_body.load(Ordering::Relaxed),
            },
            upstream_retryable_events: FilmuvfsUpstreamRetryableStatusSnapshot {
                network: self.upstream_retryable_network.load(Ordering::Relaxed),
                read_body: self.upstream_retryable_read_body.load(Ordering::Relaxed),
                status_too_many_requests: self
                    .upstream_retryable_status_too_many_requests
                    .load(Ordering::Relaxed),
                status_server_error: self
                    .upstream_retryable_status_server_error
                    .load(Ordering::Relaxed),
            },
            backend_fallback: FilmuvfsBackendFallbackStatusSnapshot {
                attempts: self.backend_fallback_attempts.load(Ordering::Relaxed),
                success: self.backend_fallback_success.load(Ordering::Relaxed),
                failure: self.backend_fallback_failure.load(Ordering::Relaxed),
                attempts_direct_read_failure: self
                    .backend_fallback_attempts_direct_read_failure
                    .load(Ordering::Relaxed),
                attempts_inline_refresh_unavailable: self
                    .backend_fallback_attempts_inline_refresh_unavailable
                    .load(Ordering::Relaxed),
                attempts_post_inline_refresh_failure: self
                    .backend_fallback_attempts_post_inline_refresh_failure
                    .load(Ordering::Relaxed),
                success_direct_read_failure: self
                    .backend_fallback_success_direct_read_failure
                    .load(Ordering::Relaxed),
                success_inline_refresh_unavailable: self
                    .backend_fallback_success_inline_refresh_unavailable
                    .load(Ordering::Relaxed),
                success_post_inline_refresh_failure: self
                    .backend_fallback_success_post_inline_refresh_failure
                    .load(Ordering::Relaxed),
                failure_direct_read_failure: self
                    .backend_fallback_failure_direct_read_failure
                    .load(Ordering::Relaxed),
                failure_inline_refresh_unavailable: self
                    .backend_fallback_failure_inline_refresh_unavailable
                    .load(Ordering::Relaxed),
                failure_post_inline_refresh_failure: self
                    .backend_fallback_failure_post_inline_refresh_failure
                    .load(Ordering::Relaxed),
            },
            chunk_cache: FilmuvfsChunkCacheStatusSnapshot {
                backend: chunk_cache_snapshot.backend,
                total_events: self.chunk_cache_hits.load(Ordering::Relaxed)
                    + self.chunk_cache_misses.load(Ordering::Relaxed)
                    + self.chunk_cache_inserts.load(Ordering::Relaxed)
                    + self.chunk_cache_prefetch_hits.load(Ordering::Relaxed),
                hits: self.chunk_cache_hits.load(Ordering::Relaxed),
                misses: self.chunk_cache_misses.load(Ordering::Relaxed),
                inserts: self.chunk_cache_inserts.load(Ordering::Relaxed),
                prefetch_hits: self.chunk_cache_prefetch_hits.load(Ordering::Relaxed),
                memory_bytes: chunk_cache_snapshot.memory_bytes,
                memory_max_bytes: chunk_cache_snapshot.memory_max_bytes,
                memory_hits: chunk_cache_snapshot.memory_hits,
                memory_misses: chunk_cache_snapshot.memory_misses,
                disk_bytes: chunk_cache_snapshot.disk_bytes,
                disk_max_bytes: chunk_cache_snapshot.disk_max_bytes,
                disk_hits: chunk_cache_snapshot.disk_hits,
                disk_misses: chunk_cache_snapshot.disk_misses,
                disk_writes: chunk_cache_snapshot.disk_writes,
                disk_write_errors: chunk_cache_snapshot.disk_write_errors,
                disk_evictions: chunk_cache_snapshot.disk_evictions,
            },
            chunk_read_patterns: FilmuvfsChunkReadPatternSnapshot {
                header_scan: self.read_pattern_header_scan.load(Ordering::Relaxed),
                sequential_scan: self.read_pattern_sequential_scan.load(Ordering::Relaxed),
                random_access: self.read_pattern_random_access.load(Ordering::Relaxed),
                tail_probe: self.read_pattern_tail_probe.load(Ordering::Relaxed),
                cache_hit: self.read_pattern_cache_hit.load(Ordering::Relaxed),
            },
            prefetch: FilmuvfsPrefetchStatusSnapshot {
                request_cache_hit: self.prefetch_request_cache_hit.load(Ordering::Relaxed),
                background_spawned: self.prefetch_background_spawned.load(Ordering::Relaxed),
                background_populated: self.prefetch_background_populated.load(Ordering::Relaxed),
                background_backpressure: self
                    .prefetch_background_backpressure
                    .load(Ordering::Relaxed),
                fairness_denied: prefetch_snapshot.fairness_denied_total,
                global_backpressure_denied: prefetch_snapshot.global_backpressure_denied_total,
                background_error: self.prefetch_background_error.load(Ordering::Relaxed),
                skipped_pattern: self.prefetch_skipped_pattern.load(Ordering::Relaxed),
                skipped_cached: self.prefetch_skipped_cached.load(Ordering::Relaxed),
                adaptive_scheduled: self.prefetch_adaptive_scheduled.load(Ordering::Relaxed),
                adaptive_error: self.prefetch_adaptive_error.load(Ordering::Relaxed),
                startup_scheduled: self.prefetch_startup_scheduled.load(Ordering::Relaxed),
                startup_error: self.prefetch_startup_error.load(Ordering::Relaxed),
                concurrency_limit: prefetch_snapshot.concurrency_limit,
                max_background_per_handle: prefetch_snapshot.max_background_per_handle,
                available_permits: prefetch_snapshot.available_permits,
                active_permits: prefetch_snapshot.active_permits,
                active_background_tasks: prefetch_snapshot.active_background_tasks,
                peak_active_background_tasks: prefetch_snapshot.peak_active_background_tasks,
                handles_with_background_tasks: prefetch_snapshot.handles_with_background_tasks,
            },
            chunk_coalescing: FilmuvfsChunkCoalescingStatusSnapshot {
                in_flight_chunks: chunk_coalescing_snapshot.in_flight_chunks,
                peak_in_flight_chunks: chunk_coalescing_snapshot.peak_in_flight_chunks,
                waits_total: chunk_coalescing_snapshot.waits_total,
                waits_hit: chunk_coalescing_snapshot.waits_hit,
                waits_miss: chunk_coalescing_snapshot.waits_miss,
                wait_average_duration_ms: chunk_coalescing_snapshot.wait_average_duration_ms,
                wait_max_duration_ms: chunk_coalescing_snapshot.wait_max_duration_ms,
            },
            inline_refresh: FilmuvfsInlineRefreshStatusSnapshot {
                success: self.inline_refresh_success.load(Ordering::Relaxed),
                no_url: self.inline_refresh_no_url.load(Ordering::Relaxed),
                error: self.inline_refresh_error.load(Ordering::Relaxed),
                timeout: self.inline_refresh_timeout.load(Ordering::Relaxed),
                skipped_missing_provider_file_id: self
                    .inline_refresh_skipped_missing_provider_file_id
                    .load(Ordering::Relaxed),
                reused_catalog_url: self
                    .inline_refresh_reused_catalog_url
                    .load(Ordering::Relaxed),
                dedup_wait: self.inline_refresh_dedup_wait.load(Ordering::Relaxed),
            },
            windows_projfs: FilmuvfsWindowsProjfsStatusSnapshot {
                callbacks_ok: self.windows_projfs_callbacks_ok.load(Ordering::Relaxed),
                callbacks_error: self.windows_projfs_callbacks_error.load(Ordering::Relaxed),
                callbacks_estale: self.windows_projfs_callbacks_estale.load(Ordering::Relaxed),
                callbacks_cancelled: self
                    .windows_projfs_callbacks_cancelled
                    .load(Ordering::Relaxed),
                callback_count,
                callback_average_ms: atomic_average_millis(callback_total, callback_count),
                callback_max_ms: atomic_max_millis(callback_max),
                stream_handles_opened: self
                    .windows_projfs_stream_handle_opened
                    .load(Ordering::Relaxed),
                stream_handles_reused: self
                    .windows_projfs_stream_handle_reused
                    .load(Ordering::Relaxed),
                stream_handles_reused_race: self
                    .windows_projfs_stream_handle_reused_race
                    .load(Ordering::Relaxed),
                stream_handles_released: self
                    .windows_projfs_stream_handle_released
                    .load(Ordering::Relaxed),
                stream_handles_released_on_shutdown: self
                    .windows_projfs_stream_handle_released_on_shutdown
                    .load(Ordering::Relaxed),
                notifications_closed_clean: self
                    .windows_projfs_notification_closed_clean
                    .load(Ordering::Relaxed),
                notifications_closed_modified: self
                    .windows_projfs_notification_closed_modified
                    .load(Ordering::Relaxed),
                notifications_closed_deleted: self
                    .windows_projfs_notification_closed_deleted
                    .load(Ordering::Relaxed),
                notifications_other: self
                    .windows_projfs_notification_other
                    .load(Ordering::Relaxed),
            },
        }
    }

    fn write_runtime_status_snapshot(&self, path: &Path, config: &SidecarConfig) -> Result<()> {
        atomic_write_json(path, &self.build_runtime_status_snapshot(config))
    }
}

pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl TelemetryGuard {
    pub fn init(
        config: &SidecarConfig,
        catalog_state: Arc<CatalogStateStore>,
        mount_runtime: Arc<MountRuntime>,
    ) -> Result<Self> {
        let filter = EnvFilter::new(config.log_filter.clone());
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_thread_names(true);

        let resource = Resource::builder()
            .with_service_name(SERVICE_NAME)
            .with_attributes([
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
                KeyValue::new("service.instance.id", config.daemon_id.clone()),
                KeyValue::new("vfs.daemon_id", config.daemon_id.clone()),
                KeyValue::new("vfs.session_id", config.session_id.clone()),
            ])
            .build();
        global::set_text_map_propagator(TraceContextPropagator::new());

        let mut tracer_provider = None;
        let mut meter_provider = None;

        if let Some(endpoint) = config.otlp_endpoint.as_ref() {
            std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint);
            std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", endpoint);
            std::env::set_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint);
            std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");

            let span_exporter = SpanExporter::builder().with_tonic().build()?;
            let built_tracer_provider = SdkTracerProvider::builder()
                .with_batch_exporter(span_exporter)
                .with_resource(resource.clone())
                .build();
            let tracer = built_tracer_provider.tracer(SERVICE_NAME);
            global::set_tracer_provider(built_tracer_provider.clone());

            let metric_exporter = MetricExporter::builder().with_tonic().build()?;
            let built_meter_provider = SdkMeterProvider::builder()
                .with_resource(resource.clone())
                .with_periodic_exporter(metric_exporter)
                .build();
            global::set_meter_provider(built_meter_provider.clone());

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .try_init()?;

            tracer_provider = Some(built_tracer_provider);
            meter_provider = Some(built_meter_provider);
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .try_init()?;
        }

        let meter = global::meter(SERVICE_NAME);
        let _ = FILMUVFS_METRICS.set(FilmuvfsMetrics::new(meter, catalog_state, mount_runtime));

        Ok(Self {
            tracer_provider,
            meter_provider,
        })
    }

    pub fn shutdown(self) -> Result<()> {
        if let Some(provider) = self.meter_provider {
            provider.shutdown()?;
        }
        if let Some(provider) = self.tracer_provider {
            provider.shutdown()?;
        }
        Ok(())
    }
}
