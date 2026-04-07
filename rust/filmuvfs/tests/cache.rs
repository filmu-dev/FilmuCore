use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use filmuvfs::cache::{CacheEngine, HybridCache, MemoryCache};
use tempfile::tempdir;

#[tokio::test]
async fn test_memory_cache_hit_returns_bytes() {
    let cache = MemoryCache::new(1024 * 1024);

    cache
        .insert("movie:0:1024".to_owned(), Bytes::from_static(b"payload"))
        .await;

    assert_eq!(
        cache.get("movie:0:1024").await,
        Some(Bytes::from_static(b"payload"))
    );
}

#[tokio::test]
async fn test_memory_cache_miss_returns_none() {
    let cache = MemoryCache::new(1024 * 1024);

    assert!(cache.get("missing").await.is_none());
}

#[tokio::test]
async fn test_memory_cache_evicts_on_capacity() {
    let cache = MemoryCache::new(3);

    cache
        .insert("k1".to_owned(), Bytes::from_static(b"aa"))
        .await;
    cache
        .insert("k2".to_owned(), Bytes::from_static(b"bb"))
        .await;
    tokio::time::sleep(Duration::from_millis(25)).await;

    assert_eq!(cache.max_bytes(), 3);
    assert!(cache.current_size_bytes() <= 3);
    assert!(cache.get("k2").await.is_some());
    assert!(cache.get("k1").await.is_none());
}

#[tokio::test]
async fn test_hybrid_cache_l2_promotion_on_hit() {
    let tempdir = tempdir().expect("temp dir should create");
    let cache_path = tempdir.path().join("hybrid-cache");

    let cache = HybridCache::new(1024 * 1024, cache_path.clone(), 16 * 1024 * 1024)
        .expect("hybrid cache should initialize");
    cache
        .insert("movie:0:1024".to_owned(), Bytes::from_static(b"payload"))
        .await;
    drop(cache);

    let cache = HybridCache::new(1024 * 1024, cache_path, 16 * 1024 * 1024)
        .expect("hybrid cache should reopen");

    assert_eq!(cache.l1_size_bytes(), 0);
    assert_eq!(
        cache.get("movie:0:1024").await,
        Some(Bytes::from_static(b"payload"))
    );
    assert!(cache.l1_size_bytes() > 0);
}

#[tokio::test]
async fn test_hybrid_cache_invalidate_clears_both_layers() {
    let tempdir = tempdir().expect("temp dir should create");
    let cache_path = tempdir.path().join("hybrid-cache");

    let cache = HybridCache::new(1024 * 1024, cache_path.clone(), 16 * 1024 * 1024)
        .expect("hybrid cache should initialize");
    cache
        .insert("movie:0:1024".to_owned(), Bytes::from_static(b"payload"))
        .await;

    cache.invalidate("movie:0:1024").await;
    assert!(cache.get("movie:0:1024").await.is_none());
    assert_eq!(cache.l1_size_bytes(), 0);
    assert_eq!(cache.l2_size_bytes(), 0);
    drop(cache);

    let cache = HybridCache::new(1024 * 1024, cache_path, 16 * 1024 * 1024)
        .expect("hybrid cache should reopen");
    assert!(cache.get("movie:0:1024").await.is_none());
}

#[tokio::test]
async fn test_cache_engine_trait_object_safe() {
    let cache: Arc<dyn CacheEngine> = Arc::new(MemoryCache::new(1024 * 1024));

    cache
        .insert("movie:0:1024".to_owned(), Bytes::from_static(b"payload"))
        .await;

    assert!(cache.get("movie:0:1024").await.is_some());
}
