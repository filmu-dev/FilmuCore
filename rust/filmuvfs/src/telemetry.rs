use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};

use anyhow::Result;
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter, ObservableGauge},
    trace::TracerProvider as _,
    KeyValue,
};
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::SdkTracerProvider, Resource};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::{
    catalog::state::CatalogStateStore, config::SidecarConfig, mount::MountRuntime, SERVICE_NAME,
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

pub struct FilmuvfsMetrics {
    read_requests_total: Counter<u64>,
    mounted_read_duration_seconds: Histogram<f64>,
    upstream_fetch_bytes_total: Counter<u64>,
    upstream_fetch_duration_seconds: Histogram<f64>,
    chunk_cache_events_total: Counter<u64>,
    chunk_read_patterns_total: Counter<u64>,
    prefetch_events_total: Counter<u64>,
    inline_refresh_total: Counter<u64>,
    windows_projfs_callbacks_total: Counter<u64>,
    windows_projfs_callback_duration_seconds: Histogram<f64>,
    windows_projfs_stream_handle_events_total: Counter<u64>,
    windows_projfs_notifications_total: Counter<u64>,
    windows_projfs_callbacks_ok: AtomicU64,
    windows_projfs_callbacks_error: AtomicU64,
    windows_projfs_callbacks_estale: AtomicU64,
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
            read_requests_total,
            mounted_read_duration_seconds,
            upstream_fetch_bytes_total,
            upstream_fetch_duration_seconds,
            chunk_cache_events_total,
            chunk_read_patterns_total,
            prefetch_events_total,
            inline_refresh_total,
            windows_projfs_callbacks_total,
            windows_projfs_callback_duration_seconds,
            windows_projfs_stream_handle_events_total,
            windows_projfs_notifications_total,
            windows_projfs_callbacks_ok: AtomicU64::new(0),
            windows_projfs_callbacks_error: AtomicU64::new(0),
            windows_projfs_callbacks_estale: AtomicU64::new(0),
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
    }

    fn record_mounted_read_duration(&self, duration: Duration, result: &'static str) {
        self.mounted_read_duration_seconds
            .record(duration.as_secs_f64(), &[KeyValue::new("result", result)]);
    }

    fn record_upstream_fetch_bytes(&self, bytes: u64) {
        if bytes > 0 {
            self.upstream_fetch_bytes_total.add(bytes, &[]);
        }
    }

    fn record_upstream_fetch_duration(&self, duration: Duration) {
        self.upstream_fetch_duration_seconds
            .record(duration.as_secs_f64(), &[]);
    }

    fn record_chunk_cache_event(&self, event: &'static str) {
        self.chunk_cache_events_total
            .add(1, &[KeyValue::new("event", event)]);
    }

    fn record_chunk_read_pattern(&self, pattern: &'static str) {
        self.chunk_read_patterns_total
            .add(1, &[KeyValue::new("pattern", pattern)]);
    }

    fn record_prefetch_event(&self, event: &'static str) {
        self.prefetch_events_total
            .add(1, &[KeyValue::new("event", event)]);
    }

    fn record_inline_refresh(&self, result: &'static str) {
        self.inline_refresh_total
            .add(1, &[KeyValue::new("result", result)]);
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
        if callback_count == 0 && opened == 0 && reused == 0 && notifications == 0 {
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
                KeyValue::new("filmuvfs.session_id", config.session_id.clone()),
            ])
            .build();

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
