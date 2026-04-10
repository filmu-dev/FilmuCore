use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use thiserror::Error;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum PrefetchScheduleError {
    #[error("prefetch concurrency must be greater than zero")]
    InvalidConcurrency,
    #[error("foreground read permit acquisition failed because the scheduler is closed")]
    SchedulerClosed,
}

#[derive(Clone, Debug)]
pub struct PrefetchScheduler {
    semaphore: Arc<Semaphore>,
    handle_tokens: Arc<DashMap<String, CancellationToken>>,
    concurrency: usize,
    active_background_tasks: Arc<AtomicU64>,
    peak_active_background_tasks: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchSchedulerSnapshot {
    pub concurrency_limit: u64,
    pub available_permits: u64,
    pub active_permits: u64,
    pub active_background_tasks: u64,
    pub peak_active_background_tasks: u64,
}

#[derive(Debug, Clone)]
pub struct VelocityTracker {
    last_read_at: Instant,
    last_offset: Option<u64>,
    last_end_exclusive: Option<u64>,
    bytes_per_sec: f64,
    sequential_streak: u32,
    prefetch_chunks: u32,
    min_prefetch: u32,
    max_prefetch: u32,
}

impl VelocityTracker {
    #[must_use]
    pub fn new(min_prefetch: u32, max_prefetch: u32) -> Self {
        Self {
            last_read_at: Instant::now(),
            last_offset: None,
            last_end_exclusive: None,
            bytes_per_sec: 0.0,
            sequential_streak: 0,
            prefetch_chunks: min_prefetch,
            min_prefetch,
            max_prefetch,
        }
    }

    #[must_use]
    pub fn update(&mut self, offset: u64, bytes_read: u64) -> u32 {
        let now = Instant::now();
        let elapsed = now
            .saturating_duration_since(self.last_read_at)
            .as_secs_f64();
        let is_sequential = match (self.last_offset, self.last_end_exclusive) {
            (None, None) => true,
            (Some(last_offset), Some(last_end_exclusive)) => {
                offset == last_end_exclusive || offset == last_offset
            }
            _ => true,
        };

        if !is_sequential {
            self.sequential_streak = 0;
            self.prefetch_chunks = self.min_prefetch;
            self.bytes_per_sec *= 0.5;
            self.last_offset = Some(offset);
            self.last_end_exclusive = Some(offset.saturating_add(bytes_read));
            self.last_read_at = now;
            return self.prefetch_chunks;
        }

        self.sequential_streak = if self.last_offset.is_none() {
            1
        } else {
            self.sequential_streak.saturating_add(1)
        };

        if elapsed > 0.0 && bytes_read > 0 {
            let instant_bps = bytes_read as f64 / elapsed.max(0.001);
            self.bytes_per_sec = if self.bytes_per_sec == 0.0 {
                instant_bps
            } else {
                (self.bytes_per_sec * 0.8) + (instant_bps * 0.2)
            };
        }

        if self.sequential_streak > 0 && self.sequential_streak.is_multiple_of(4) {
            self.prefetch_chunks = self
                .prefetch_chunks
                .saturating_mul(2)
                .clamp(self.min_prefetch, self.max_prefetch);
        }

        self.last_offset = Some(offset);
        self.last_end_exclusive = Some(offset.saturating_add(bytes_read));
        self.last_read_at = now;
        self.prefetch_chunks
    }

    #[must_use]
    pub fn bytes_per_sec(&self) -> f64 {
        self.bytes_per_sec
    }

    #[must_use]
    pub fn prefetch_chunks(&self) -> u32 {
        self.prefetch_chunks
    }
}

impl PrefetchScheduler {
    pub fn new(concurrency: usize) -> Result<Self, PrefetchScheduleError> {
        if concurrency == 0 {
            return Err(PrefetchScheduleError::InvalidConcurrency);
        }

        Ok(Self {
            semaphore: Arc::new(Semaphore::new(concurrency)),
            handle_tokens: Arc::new(DashMap::new()),
            concurrency,
            active_background_tasks: Arc::new(AtomicU64::new(0)),
            peak_active_background_tasks: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn register_handle(&self, handle_key: &str) {
        self.handle_tokens.entry(handle_key.to_owned()).or_default();
    }

    pub fn cancel_handle(&self, handle_key: &str) {
        if let Some((_, token)) = self.handle_tokens.remove(handle_key) {
            token.cancel();
        }
    }

    pub async fn acquire_foreground(&self) -> Result<OwnedSemaphorePermit, PrefetchScheduleError> {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| PrefetchScheduleError::SchedulerClosed)
    }

    #[must_use]
    pub fn snapshot(&self) -> PrefetchSchedulerSnapshot {
        let concurrency_limit = self.concurrency as u64;
        let available_permits = self.semaphore.available_permits() as u64;
        PrefetchSchedulerSnapshot {
            concurrency_limit,
            available_permits,
            active_permits: concurrency_limit.saturating_sub(available_permits),
            active_background_tasks: self.active_background_tasks.load(Ordering::Relaxed),
            peak_active_background_tasks: self.peak_active_background_tasks.load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub fn spawn_background<Fut>(&self, handle_key: &str, future: Fut) -> bool
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let token = self
            .handle_tokens
            .entry(handle_key.to_owned())
            .or_default()
            .clone();
        let Ok(permit) = self.semaphore.clone().try_acquire_owned() else {
            return false;
        };
        let active_background_tasks = Arc::clone(&self.active_background_tasks);
        let active_count = active_background_tasks.fetch_add(1, Ordering::Relaxed) + 1;
        update_max(&self.peak_active_background_tasks, active_count);

        tokio::spawn(async move {
            let _active_background_guard =
                BackgroundTaskGuard::new(Arc::clone(&active_background_tasks));
            tokio::select! {
                _ = token.cancelled() => {}
                _ = async move {
                    let _permit = permit;
                    future.await;
                } => {}
            }
        });
        true
    }
}

#[derive(Debug)]
struct BackgroundTaskGuard {
    active_background_tasks: Arc<AtomicU64>,
}

impl BackgroundTaskGuard {
    fn new(active_background_tasks: Arc<AtomicU64>) -> Self {
        Self {
            active_background_tasks,
        }
    }
}

impl Drop for BackgroundTaskGuard {
    fn drop(&mut self) {
        self.active_background_tasks.fetch_sub(1, Ordering::Relaxed);
    }
}

fn update_max(target: &AtomicU64, candidate: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        (candidate > current).then_some(candidate)
    });
}
