use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use filmuvfs::{
    cache::{CacheEngine, MemoryCache},
    chunk_engine::{ChunkEngine, ChunkEngineConfig, ChunkReadRequest},
    chunk_planner::ChunkPlannerConfig,
    prefetch::VelocityTracker,
    upstream::UpstreamReader,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::Duration,
};

fn patterned_bytes(length: usize) -> Vec<u8> {
    (0..length).map(|index| (index % 251) as u8).collect()
}

async fn spawn_range_response_server(
    body: Vec<u8>,
) -> (
    String,
    Arc<AtomicUsize>,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener address should resolve");
    let request_count = Arc::new(AtomicUsize::new(0));
    let body = Arc::new(body);
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let task = tokio::spawn({
        let body = Arc::clone(&body);
        let request_count = Arc::clone(&request_count);
        async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.expect("range server should accept a client");
                        let mut request_buffer = vec![0_u8; 4096];
                        let request_len = socket
                            .read(&mut request_buffer)
                            .await
                            .expect("request should be readable");
                        let request_text = String::from_utf8_lossy(&request_buffer[..request_len]);
                        let (start, end_exclusive) = parse_requested_range(&request_text, body.len());
                        let payload = &body[start..end_exclusive];

                        request_count.fetch_add(1, Ordering::SeqCst);

                        let response = format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            payload.len()
                        );
                        socket
                            .write_all(response.as_bytes())
                            .await
                            .expect("response head should write");
                        socket
                            .write_all(payload)
                            .await
                            .expect("response body should write");
                    }
                }
            }
        }
    });

    (
        format!("http://{address}/movie.mkv"),
        request_count,
        shutdown_tx,
        task,
    )
}

fn parse_requested_range(request: &str, body_length: usize) -> (usize, usize) {
    for line in request.lines() {
        if line.to_ascii_lowercase().starts_with("range: bytes=") {
            let range_value = &line["Range: bytes=".len()..];
            let (start, end_inclusive) = range_value
                .trim()
                .split_once('-')
                .expect("range header should contain start and end");
            let start = start.parse::<usize>().expect("range start should parse");
            let end_inclusive = end_inclusive
                .parse::<usize>()
                .expect("range end should parse");
            return (start, end_inclusive.saturating_add(1).min(body_length));
        }
    }

    (0, body_length)
}

#[test]
fn test_velocity_tracker_sequential_expands_window() {
    let mut tracker = VelocityTracker::new(1, 16);

    for index in 0_u64..4 {
        let _ = tracker.update(index * 1024, 1024);
    }

    assert!(tracker.prefetch_chunks() > 1);
}

#[test]
fn test_velocity_tracker_seek_resets_window() {
    let mut tracker = VelocityTracker::new(1, 16);
    for index in 0_u64..8 {
        let _ = tracker.update(index * 1024, 1024);
    }

    let _ = tracker.update(999_999, 1024);

    assert_eq!(tracker.prefetch_chunks(), 1);
}

#[test]
fn test_velocity_tracker_caps_at_max() {
    let mut tracker = VelocityTracker::new(1, 4);
    for index in 0_u64..32 {
        let _ = tracker.update(index * 1024, 1024);
    }

    assert!(tracker.prefetch_chunks() <= 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_prefetch_triggers_background_fetch() {
    let body = patterned_bytes(900_000);
    let (url, request_count, shutdown_tx, server_task) = spawn_range_response_server(body).await;
    let cache: Arc<dyn CacheEngine> = Arc::new(MemoryCache::new(1024 * 1024));
    let planner = ChunkPlannerConfig {
        scan_chunk_size: 1024,
        random_chunk_size: 512,
        sequential_prefetch_chunks: 0,
        ..ChunkPlannerConfig::default()
    };
    let engine = ChunkEngine::new(
        cache,
        ChunkEngineConfig {
            planner,
            prefetch_concurrency: 4,
        },
        UpstreamReader::new(),
    )
    .expect("chunk engine should initialize");
    let request = ChunkReadRequest {
        handle_key: "prefetch-handle".to_owned(),
        file_id: "provider-file-movie-1".to_owned(),
        url,
        provider_file_id: Some("provider-file-movie-1".to_owned()),
        offset: 131_072,
        length: 1024,
        file_size: Some(900_000),
    };

    let bytes = engine
        .read(request.clone())
        .await
        .expect("foreground read should succeed");
    assert_eq!(bytes.len(), 1024);
    assert_eq!(request_count.load(Ordering::SeqCst), 1);

    engine
        .prefetch_ahead(request, 1)
        .await
        .expect("adaptive prefetch should schedule");
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(request_count.load(Ordering::SeqCst) >= 2);

    let _ = shutdown_tx.send(());
    server_task
        .await
        .expect("range server task should shut down cleanly");
}
