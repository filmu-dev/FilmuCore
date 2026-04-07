mod common;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use bytes::Bytes;
use filmuvfs::{
    catalog::state::CatalogStateStore,
    mount::{CatalogEntryRefreshClient, MountRuntime, MountRuntimeError},
    proto::catalog_entry::Details as CatalogEntryDetails,
    upstream::RangeRequest,
};
use hyper::header::RANGE;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

fn runtime_with_movie_url(url: String) -> (MountRuntime, u64, u64) {
    runtime_with_movie_url_and_size(url, 1_024)
}

fn runtime_with_movie_url_and_size(url: String, movie_size: u64) -> (MountRuntime, u64, u64) {
    let state = seeded_state_with_movie_size(&url, movie_size);
    let runtime = MountRuntime::new(Arc::clone(&state), "session-read-tests".to_owned());
    let movie_inode = common::movie_inode();
    let handle = runtime
        .open_by_inode(movie_inode)
        .expect("open_by_inode should succeed");
    (runtime, movie_inode, handle.handle_id)
}

fn seeded_state_with_movie_size(movie_url: &str, movie_size: u64) -> Arc<CatalogStateStore> {
    let state = Arc::new(CatalogStateStore::new());
    let mut snapshot =
        common::sample_catalog_snapshot(movie_url, "http://127.0.0.1:18080/episode.mkv");
    let movie_entry = snapshot
        .entries
        .iter_mut()
        .find(|entry| entry.entry_id == common::MOVIE_FILE_ENTRY_ID)
        .expect("movie entry should exist in the seeded snapshot");

    match movie_entry.details.as_mut() {
        Some(CatalogEntryDetails::File(file)) => {
            file.size_bytes = Some(
                movie_size
                    .try_into()
                    .expect("movie size should fit into the proto size field"),
            );
        }
        other => panic!("expected file details for movie entry, got {other:?}"),
    }

    state
        .apply_snapshot(snapshot)
        .expect("custom snapshot should apply");
    state
}

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

async fn spawn_single_response_server(
    status_line: &str,
    body: &'static [u8],
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener address should resolve");
    let status_line = status_line.to_owned();
    let payload = body.to_vec();

    let task = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .expect("server should accept one client");
        let mut request_buffer = vec![0_u8; 4096];
        let _ = socket.read(&mut request_buffer).await;

        let response = format!(
            "HTTP/1.1 {status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("response head should write");
        socket
            .write_all(&payload)
            .await
            .expect("response body should write");
    });

    (format!("http://{address}/movie.mkv"), task)
}

#[test]
fn range_request_construction_uses_offset_and_size() {
    let request = RangeRequest::new("http://127.0.0.1:18080/movie.mkv".to_owned(), 128, 512);
    let http_request = request
        .build_http_request()
        .expect("HTTP request should be constructible");

    assert_eq!(request.range_header_value(), "bytes=128-639");
    assert_eq!(
        http_request
            .headers()
            .get(RANGE)
            .expect("Range header should be present"),
        "bytes=128-639"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_http_statuses_map_to_estale() {
    for (status_line, expected_status_code) in [
        ("401 Unauthorized", 401_u16),
        ("403 Forbidden", 403_u16),
        ("410 Gone", 410_u16),
    ] {
        let (url, server_task) = spawn_single_response_server(status_line, b"stale").await;
        let (runtime, movie_inode, handle_id) = runtime_with_movie_url(url);

        let error = runtime
            .read_bytes(handle_id, movie_inode, 0, 64)
            .await
            .expect_err("stale upstream statuses should fail the read");

        match error {
            MountRuntimeError::StaleLease { status_code, .. } => {
                assert_eq!(status_code, expected_status_code)
            }
            other => panic!("expected stale lease error, got {other:?}"),
        }

        server_task
            .await
            .expect("server task should finish cleanly");
    }
}

#[derive(Clone)]
struct FakeRefreshClient {
    refreshed_url: String,
    calls: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl CatalogEntryRefreshClient for FakeRefreshClient {
    async fn refresh_catalog_entry(
        &self,
        provider_file_id: &str,
        handle_key: &str,
        entry_id: &str,
    ) -> Result<Option<String>, String> {
        assert_eq!(provider_file_id, "provider-file-movie-1");
        assert_eq!(handle_key, "session-read-tests:1");
        assert_eq!(entry_id, common::MOVIE_FILE_ENTRY_ID);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(self.refreshed_url.clone()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn network_errors_map_to_eio_class_errors() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral port should bind");
    let address = listener
        .local_addr()
        .expect("listener address should resolve");
    drop(listener);

    let (runtime, movie_inode, handle_id) =
        runtime_with_movie_url(format!("http://{address}/movie.mkv"));
    let error = runtime
        .read_bytes(handle_id, movie_inode, 0, 64)
        .await
        .expect_err("connection refusal should map to an I/O error");

    match error {
        MountRuntimeError::Io { path, .. } => {
            assert_eq!(
                path,
                "/movies/Example Movie (2024)/Example Movie (2024).mkv"
            );
        }
        other => panic!("expected I/O error, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_http_status_triggers_inline_refresh_and_read_succeeds() {
    let fresh_body = patterned_bytes(1_024);
    let expected = Bytes::copy_from_slice(&fresh_body[..64]);
    let (stale_url, stale_server_task) =
        spawn_single_response_server("401 Unauthorized", b"stale").await;
    let (fresh_url, fresh_request_count, shutdown_tx, fresh_server_task) =
        spawn_range_response_server(fresh_body).await;

    let refresh_calls = Arc::new(AtomicUsize::new(0));
    let refresh_client = Arc::new(FakeRefreshClient {
        refreshed_url: fresh_url.clone(),
        calls: Arc::clone(&refresh_calls),
    });

    let state = common::seeded_state(&stale_url, "http://127.0.0.1:18080/episode.mkv");
    let runtime = MountRuntime::new(Arc::clone(&state), "session-read-tests".to_owned());
    runtime.set_refresh_client(refresh_client);
    let movie_inode = common::movie_inode();
    let handle = runtime
        .open_by_inode(movie_inode)
        .expect("open_by_inode should succeed");

    let bytes = runtime
        .read_bytes(handle.handle_id, movie_inode, 0, 64)
        .await
        .expect("stale upstream response should be retried with a refreshed URL");

    assert_eq!(bytes, expected);
    assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fresh_request_count.load(Ordering::SeqCst), 1);

    let refreshed_request = runtime
        .prepare_read_request(handle.handle_id, movie_inode, 0, 64)
        .expect("refreshed URL should be visible to subsequent reads");
    assert_eq!(refreshed_request.unrestricted_url, fresh_url);

    stale_server_task
        .await
        .expect("stale server task should finish cleanly");
    let _ = shutdown_tx.send(());
    fresh_server_task
        .await
        .expect("fresh server task should finish cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_aligned_read_returns_correct_bytes() {
    let body = patterned_bytes(900_000);
    let offset = 131_072_u64;
    let length = 4_096_u32;
    let expected =
        Bytes::copy_from_slice(&body[offset as usize..offset as usize + length as usize]);
    let (url, request_count, shutdown_tx, server_task) = spawn_range_response_server(body).await;
    let (runtime, movie_inode, handle_id) = runtime_with_movie_url_and_size(url, 900_000);

    let bytes = runtime
        .read_bytes(handle_id, movie_inode, offset, length)
        .await
        .expect("chunk-aligned read should succeed");

    assert_eq!(bytes, expected);
    assert_eq!(request_count.load(Ordering::SeqCst), 1);

    let _ = shutdown_tx.send(());
    server_task
        .await
        .expect("range server task should shut down cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_chunk_boundary_read_stitches() {
    let body = patterned_bytes(900_000);
    let offset = 131_072_u64 + 262_144_u64 - 32;
    let length = 128_u32;
    let expected =
        Bytes::copy_from_slice(&body[offset as usize..offset as usize + length as usize]);
    let (url, request_count, shutdown_tx, server_task) = spawn_range_response_server(body).await;
    let (runtime, movie_inode, handle_id) = runtime_with_movie_url_and_size(url, 900_000);

    let bytes = runtime
        .read_bytes(handle_id, movie_inode, offset, length)
        .await
        .expect("cross-chunk read should succeed");

    assert_eq!(bytes, expected);
    assert_eq!(request_count.load(Ordering::SeqCst), 2);

    let _ = shutdown_tx.send(());
    server_task
        .await
        .expect("range server task should shut down cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_hit_skips_upstream() {
    let body = patterned_bytes(900_000);
    let (url, request_count, shutdown_tx, server_task) = spawn_range_response_server(body).await;
    let (runtime, movie_inode, handle_id) = runtime_with_movie_url_and_size(url, 900_000);

    let first = runtime
        .read_bytes(handle_id, movie_inode, 131_072, 4_096)
        .await
        .expect("initial read should succeed");
    let second = runtime
        .read_bytes(handle_id, movie_inode, 131_072, 4_096)
        .await
        .expect("cached read should succeed");

    assert_eq!(first, second);
    assert_eq!(request_count.load(Ordering::SeqCst), 1);

    let _ = shutdown_tx.send(());
    server_task
        .await
        .expect("range server task should shut down cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_beyond_eof_is_clamped() {
    let body = patterned_bytes(1_024);
    let offset = 1_000_u64;
    let expected = Bytes::copy_from_slice(&body[offset as usize..]);
    let (url, request_count, shutdown_tx, server_task) = spawn_range_response_server(body).await;
    let (runtime, movie_inode, handle_id) = runtime_with_movie_url(url);

    let bytes = runtime
        .read_bytes(handle_id, movie_inode, offset, 128)
        .await
        .expect("EOF-clamped read should succeed");

    assert_eq!(bytes, expected);
    assert_eq!(request_count.load(Ordering::SeqCst), 1);

    let _ = shutdown_tx.send(());
    server_task
        .await
        .expect("range server task should shut down cleanly");
}
