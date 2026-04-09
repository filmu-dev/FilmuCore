use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use moka::{future::Cache, notification::RemovalCause};
use tokio::sync::Mutex;
use tracing::warn;

const DISK_DATA_TREE: &str = "cache_data";
const DISK_META_TREE: &str = "cache_meta";
const DISK_LRU_TREE: &str = "cache_lru";

/// Trait-based cache abstraction compiled into the binary.
#[async_trait]
pub trait CacheEngine: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Option<Bytes>;
    async fn insert(&self, key: String, value: Bytes);
    async fn invalidate(&self, key: &str);
    fn size_bytes(&self) -> u64;
    fn name(&self) -> &'static str;
    fn snapshot(&self) -> CacheEngineSnapshot;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheEngineSnapshot {
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

#[derive(Debug)]
pub struct MemoryCache {
    inner: Cache<String, Bytes>,
    size_bytes: Arc<AtomicU64>,
    max_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl MemoryCache {
    #[must_use]
    pub fn new(max_bytes: u64) -> Self {
        let size_bytes = Arc::new(AtomicU64::new(0));
        let size_bytes_on_evict = Arc::clone(&size_bytes);
        let inner = Cache::builder()
            .weigher(|_: &String, value: &Bytes| value.len() as u32)
            .max_capacity(max_bytes)
            .eviction_listener(move |_key, value, cause| {
                if !matches!(cause, RemovalCause::Replaced) {
                    subtract_bytes(&size_bytes_on_evict, value.len() as u64);
                }
            })
            .build();

        Self {
            inner,
            size_bytes,
            max_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    #[must_use]
    pub fn current_size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl CacheEngine for MemoryCache {
    async fn get(&self, key: &str) -> Option<Bytes> {
        let value = self.inner.get(key).await;
        if value.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        value
    }

    async fn insert(&self, key: String, value: Bytes) {
        if let Some(previous) = self.inner.get(&key).await {
            subtract_bytes(&self.size_bytes, previous.len() as u64);
        }
        add_bytes(&self.size_bytes, value.len() as u64);
        self.inner.insert(key, value).await;
        self.inner.run_pending_tasks().await;
    }

    async fn invalidate(&self, key: &str) {
        if let Some(previous) = self.inner.get(key).await {
            subtract_bytes(&self.size_bytes, previous.len() as u64);
        }
        self.inner.invalidate(key).await;
        self.inner.run_pending_tasks().await;
    }

    fn size_bytes(&self) -> u64 {
        self.current_size_bytes()
    }

    fn name(&self) -> &'static str {
        "memory"
    }

    fn snapshot(&self) -> CacheEngineSnapshot {
        CacheEngineSnapshot {
            backend: self.name(),
            weighted_size_bytes: self.current_size_bytes(),
            memory_bytes: self.current_size_bytes(),
            memory_max_bytes: self.max_bytes(),
            memory_hits: self.hits.load(Ordering::Relaxed),
            memory_misses: self.misses.load(Ordering::Relaxed),
            disk_bytes: 0,
            disk_max_bytes: 0,
            disk_hits: 0,
            disk_misses: 0,
            disk_writes: 0,
            disk_write_errors: 0,
            disk_evictions: 0,
        }
    }
}

#[derive(Debug)]
pub struct HybridCache {
    l1: MemoryCache,
    db: sled::Db,
    data: sled::Tree,
    meta: sled::Tree,
    lru: sled::Tree,
    l2_max_bytes: u64,
    l2_bytes: Arc<AtomicU64>,
    access_counter: Arc<AtomicU64>,
    write_lock: Mutex<()>,
    l2_hits: AtomicU64,
    l2_misses: AtomicU64,
    l2_writes: AtomicU64,
    l2_write_errors: AtomicU64,
    l2_evictions: AtomicU64,
}

impl HybridCache {
    pub fn new(l1_max_bytes: u64, l2_path: PathBuf, l2_max_bytes: u64) -> Result<Self> {
        let db = sled::open(&l2_path)
            .with_context(|| format!("failed to open L2 cache at {}", l2_path.display()))?;
        let data = db
            .open_tree(DISK_DATA_TREE)
            .context("failed to open L2 cache data tree")?;
        let meta = db
            .open_tree(DISK_META_TREE)
            .context("failed to open L2 cache metadata tree")?;
        let lru = db
            .open_tree(DISK_LRU_TREE)
            .context("failed to open L2 cache LRU tree")?;
        let (logical_bytes, max_access_order) = load_disk_state(&meta)?;

        Ok(Self {
            l1: MemoryCache::new(l1_max_bytes),
            db,
            data,
            meta,
            lru,
            l2_max_bytes,
            l2_bytes: Arc::new(AtomicU64::new(logical_bytes)),
            access_counter: Arc::new(AtomicU64::new(max_access_order)),
            write_lock: Mutex::new(()),
            l2_hits: AtomicU64::new(0),
            l2_misses: AtomicU64::new(0),
            l2_writes: AtomicU64::new(0),
            l2_write_errors: AtomicU64::new(0),
            l2_evictions: AtomicU64::new(0),
        })
    }

    #[must_use]
    pub fn l1_size_bytes(&self) -> u64 {
        self.l1.current_size_bytes()
    }

    #[must_use]
    pub fn l2_size_bytes(&self) -> u64 {
        self.l2_bytes.load(Ordering::Relaxed)
    }

    fn next_access_order(&self) -> u64 {
        self.access_counter
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    async fn record_access(&self, key: &str) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        self.record_access_locked(key)
    }

    fn record_access_locked(&self, key: &str) -> Result<()> {
        let Some(existing) = self
            .meta
            .get(key.as_bytes())?
            .as_deref()
            .and_then(decode_metadata)
        else {
            return Ok(());
        };

        self.lru
            .remove(encode_order(existing.access_order))
            .context("failed to remove stale L2 cache LRU entry")?;
        let updated = DiskEntryMetadata {
            access_order: self.next_access_order(),
            size_bytes: existing.size_bytes,
        };
        self.meta
            .insert(key.as_bytes(), encode_metadata(updated).to_vec())
            .context("failed to update L2 cache metadata")?;
        self.lru
            .insert(encode_order(updated.access_order), key.as_bytes())
            .context("failed to update L2 cache LRU entry")?;
        Ok(())
    }

    async fn write_l2(&self, key: &str, value: &Bytes) -> Result<()> {
        {
            let _guard = self.write_lock.lock().await;
            self.write_l2_locked(key, value)?;
        }
        self.db
            .flush_async()
            .await
            .context("failed to flush L2 cache write to disk")?;
        Ok(())
    }

    fn write_l2_locked(&self, key: &str, value: &Bytes) -> Result<()> {
        if let Some(existing) = self
            .meta
            .get(key.as_bytes())?
            .as_deref()
            .and_then(decode_metadata)
        {
            self.lru
                .remove(encode_order(existing.access_order))
                .context("failed to remove replaced L2 cache LRU entry")?;
            subtract_bytes(&self.l2_bytes, existing.size_bytes);
        }

        self.data
            .insert(key.as_bytes(), value.to_vec())
            .context("failed to write value into L2 cache")?;
        let metadata = DiskEntryMetadata {
            access_order: self.next_access_order(),
            size_bytes: value.len() as u64,
        };
        self.meta
            .insert(key.as_bytes(), encode_metadata(metadata).to_vec())
            .context("failed to write L2 cache metadata")?;
        self.lru
            .insert(encode_order(metadata.access_order), key.as_bytes())
            .context("failed to write L2 cache LRU entry")?;
        add_bytes(&self.l2_bytes, metadata.size_bytes);

        self.enforce_l2_capacity_locked()
    }

    async fn remove_l2(&self, key: &str) -> Result<()> {
        {
            let _guard = self.write_lock.lock().await;
            self.remove_l2_locked(key, None)?;
        }
        self.db
            .flush_async()
            .await
            .context("failed to flush L2 cache deletion to disk")?;
        Ok(())
    }

    fn remove_l2_locked(&self, key: &str, lru_order_key: Option<&[u8]>) -> Result<()> {
        if let Some(existing) = self
            .meta
            .get(key.as_bytes())?
            .as_deref()
            .and_then(decode_metadata)
        {
            subtract_bytes(&self.l2_bytes, existing.size_bytes);
            self.meta
                .remove(key.as_bytes())
                .context("failed to remove L2 cache metadata")?;
            self.data
                .remove(key.as_bytes())
                .context("failed to remove L2 cache value")?;
            self.lru
                .remove(encode_order(existing.access_order))
                .context("failed to remove L2 cache LRU entry")?;
        }

        if let Some(order_key) = lru_order_key {
            self.lru
                .remove(order_key)
                .context("failed to remove stale L2 cache LRU key")?;
        }
        Ok(())
    }

    fn enforce_l2_capacity_locked(&self) -> Result<()> {
        while self.l2_bytes.load(Ordering::Relaxed) > self.l2_max_bytes {
            let Some(entry) = self.lru.iter().next() else {
                break;
            };
            let (order_key, key_bytes) = entry.context("failed to iterate L2 cache LRU entries")?;
            let key = String::from_utf8(key_bytes.to_vec())
                .context("encountered non-UTF8 cache key during L2 eviction")?;
            self.remove_l2_locked(&key, Some(order_key.as_ref()))?;
            self.l2_evictions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

#[async_trait]
impl CacheEngine for HybridCache {
    async fn get(&self, key: &str) -> Option<Bytes> {
        if let Some(value) = self.l1.get(key).await {
            return Some(value);
        }

        match self.data.get(key.as_bytes()) {
            Ok(Some(value)) => {
                self.l2_hits.fetch_add(1, Ordering::Relaxed);
                let bytes = Bytes::from(value.to_vec());
                self.l1.insert(key.to_owned(), bytes.clone()).await;
                if let Err(error) = self.record_access(key).await {
                    warn!(error = %error, cache_key = %key, "failed to update L2 cache access metadata");
                }
                Some(bytes)
            }
            Ok(None) => {
                self.l2_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(error) => {
                self.l2_misses.fetch_add(1, Ordering::Relaxed);
                warn!(error = %error, cache_key = %key, "failed to read from L2 cache");
                None
            }
        }
    }

    async fn insert(&self, key: String, value: Bytes) {
        self.l1.insert(key.clone(), value.clone()).await;
        if let Err(error) = self.write_l2(&key, &value).await {
            self.l2_write_errors.fetch_add(1, Ordering::Relaxed);
            warn!(error = %error, cache_key = %key, "failed to persist value into L2 cache");
        } else {
            self.l2_writes.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn invalidate(&self, key: &str) {
        self.l1.invalidate(key).await;
        if let Err(error) = self.remove_l2(key).await {
            warn!(error = %error, cache_key = %key, "failed to invalidate L2 cache entry");
        }
    }

    fn size_bytes(&self) -> u64 {
        self.l1
            .current_size_bytes()
            .saturating_add(self.l2_bytes.load(Ordering::Relaxed))
    }

    fn name(&self) -> &'static str {
        "hybrid"
    }

    fn snapshot(&self) -> CacheEngineSnapshot {
        let l1_snapshot = self.l1.snapshot();
        CacheEngineSnapshot {
            backend: self.name(),
            weighted_size_bytes: self.size_bytes(),
            memory_bytes: self.l1_size_bytes(),
            memory_max_bytes: self.l1.max_bytes(),
            memory_hits: l1_snapshot.memory_hits,
            memory_misses: l1_snapshot.memory_misses,
            disk_bytes: self.l2_size_bytes(),
            disk_max_bytes: self.l2_max_bytes,
            disk_hits: self.l2_hits.load(Ordering::Relaxed),
            disk_misses: self.l2_misses.load(Ordering::Relaxed),
            disk_writes: self.l2_writes.load(Ordering::Relaxed),
            disk_write_errors: self.l2_write_errors.load(Ordering::Relaxed),
            disk_evictions: self.l2_evictions.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DiskEntryMetadata {
    access_order: u64,
    size_bytes: u64,
}

fn load_disk_state(meta: &sled::Tree) -> Result<(u64, u64)> {
    let mut logical_bytes = 0_u64;
    let mut max_access_order = 0_u64;

    for entry in meta.iter() {
        let (_key, value) = entry.context("failed to inspect persisted L2 cache metadata")?;
        if let Some(metadata) = decode_metadata(value.as_ref()) {
            logical_bytes = logical_bytes.saturating_add(metadata.size_bytes);
            max_access_order = max_access_order.max(metadata.access_order);
        }
    }

    Ok((logical_bytes, max_access_order))
}

fn encode_order(order: u64) -> [u8; 8] {
    order.to_be_bytes()
}

fn encode_metadata(metadata: DiskEntryMetadata) -> [u8; 16] {
    let mut encoded = [0_u8; 16];
    encoded[..8].copy_from_slice(&metadata.access_order.to_be_bytes());
    encoded[8..].copy_from_slice(&metadata.size_bytes.to_be_bytes());
    encoded
}

fn decode_metadata(value: &[u8]) -> Option<DiskEntryMetadata> {
    if value.len() != 16 {
        return None;
    }

    let mut access_order = [0_u8; 8];
    access_order.copy_from_slice(&value[..8]);
    let mut size_bytes = [0_u8; 8];
    size_bytes.copy_from_slice(&value[8..]);

    Some(DiskEntryMetadata {
        access_order: u64::from_be_bytes(access_order),
        size_bytes: u64::from_be_bytes(size_bytes),
    })
}

fn add_bytes(counter: &AtomicU64, bytes: u64) {
    counter.fetch_add(bytes, Ordering::Relaxed);
}

fn subtract_bytes(counter: &AtomicU64, bytes: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(bytes))
    });
}
