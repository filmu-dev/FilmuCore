#![cfg(all(target_os = "linux", feature = "integration-tests"))]

mod common;

use std::{path::PathBuf, pin::Pin, sync::Arc, time::Duration};

use filmuvfs::{
    catalog::state::CatalogStateStore,
    config::{CacheConfig, PrefetchConfig, SidecarConfig},
    mount::{MountRuntime, Session},
    proto::filmu::vfs::catalog::v1::{
        filmu_vfs_catalog_service_server::{FilmuVfsCatalogService, FilmuVfsCatalogServiceServer},
        watch_catalog_event, watch_catalog_request, CatalogSnapshot, RefreshCatalogEntryRequest,
        RefreshCatalogEntryResponse, WatchCatalogEvent, WatchCatalogRequest,
    },
    SERVICE_NAME,
};
use tempfile::tempdir;
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::{wrappers::TcpListenerStream, Stream};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Clone)]
struct MockCatalogService {
    snapshot: CatalogSnapshot,
}

#[tonic::async_trait]
impl FilmuVfsCatalogService for MockCatalogService {
    type WatchCatalogStream =
        Pin<Box<dyn Stream<Item = Result<WatchCatalogEvent, Status>> + Send + 'static>>;

    async fn refresh_catalog_entry(
        &self,
        _request: Request<RefreshCatalogEntryRequest>,
    ) -> Result<Response<RefreshCatalogEntryResponse>, Status> {
        Ok(Response::new(RefreshCatalogEntryResponse {
            success: false,
            new_url: String::new(),
        }))
    }

    async fn watch_catalog(
        &self,
        request: Request<tonic::Streaming<WatchCatalogRequest>>,
    ) -> Result<Response<Self::WatchCatalogStream>, Status> {
        let snapshot = self.snapshot.clone();
        let mut inbound = request.into_inner();
        let stream = async_stream::stream! {
            let mut sent_snapshot = false;
            loop {
                match inbound.message().await {
                    Ok(Some(message)) => {
                        if matches!(message.command, Some(watch_catalog_request::Command::Subscribe(_))) && !sent_snapshot {
                            sent_snapshot = true;
                            yield Ok(WatchCatalogEvent {
                                event_id: "snapshot-1".to_owned(),
                                published_at: None,
                                payload: Some(watch_catalog_event::Payload::Snapshot(snapshot.clone())),
                            });
                        }
                    }
                    Ok(None) => break,
                    Err(status) => {
                        yield Err(status);
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::WatchCatalogStream))
    }
}

async fn spawn_mock_catalog_server(
    snapshot: CatalogSnapshot,
) -> (
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock gRPC server should bind");
    let address = listener
        .local_addr()
        .expect("mock gRPC address should resolve");
    let incoming = TcpListenerStream::new(listener);
    let service = MockCatalogService { snapshot };
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(FilmuVfsCatalogServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    (format!("http://{address}"), shutdown_tx, task)
}

fn integration_config(mountpoint: &std::path::Path, grpc_endpoint: &str) -> SidecarConfig {
    SidecarConfig {
        service_name: SERVICE_NAME.to_owned(),
        daemon_id: "integration-daemon".to_owned(),
        session_id: "integration-session".to_owned(),
        mountpoint: mountpoint.to_path_buf(),
        grpc_endpoint: grpc_endpoint.to_owned(),
        otlp_endpoint: None,
        log_filter: "info".to_owned(),
        connect_timeout: Duration::from_secs(5),
        rpc_timeout: Duration::from_secs(10),
        initial_catalog_sync_timeout: Duration::from_secs(30),
        heartbeat_interval: Duration::from_secs(1),
        reconnect_backoff_initial: Duration::from_secs(1),
        reconnect_backoff_max: Duration::from_secs(2),
        request_buffer: 8,
        cache: CacheConfig {
            l1_max_bytes: 500 * 1024 * 1024,
            l2_enabled: false,
            l2_path: PathBuf::new(),
            l2_max_bytes: 10 * 1024 * 1024 * 1024,
        },
        prefetch: PrefetchConfig::default(),
        prefetch_concurrency: 4,
        chunk_size_scan_kb: 1024,
        chunk_size_random_kb: 256,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mount_lifecycle_mounts_lists_and_unmounts_cleanly() {
    assert!(
        std::path::Path::new("/dev/fuse").exists(),
        "/dev/fuse must exist to run the real FUSE integration test"
    );

    let mountpoint = tempdir().expect("temp mountpoint should create");
    let snapshot = common::sample_catalog_snapshot(
        "http://127.0.0.1:18080/movie.mkv",
        "http://127.0.0.1:18080/episode.mkv",
    );
    let (grpc_endpoint, shutdown_tx, server_task) = spawn_mock_catalog_server(snapshot).await;

    let config = integration_config(mountpoint.path(), &grpc_endpoint);
    let catalog_state = Arc::new(CatalogStateStore::new());
    let mount_runtime = Arc::new(MountRuntime::new(
        Arc::clone(&catalog_state),
        config.session_id.clone(),
    ));

    let session = Session::mount(
        mountpoint.path(),
        grpc_endpoint.clone(),
        config,
        Arc::clone(&catalog_state),
        Arc::clone(&mount_runtime),
    )
    .await
    .expect("session mount should succeed against mock gRPC supplier");

    eprintln!(
        "[mount_lifecycle] mounted at {}",
        mountpoint.path().display()
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    eprintln!("[mount_lifecycle] collecting root directory listing");

    let root_listing: Vec<String> = std::fs::read_dir(mountpoint.path())
        .expect("mounted root should be readable")
        .map(|entry| {
            entry
                .expect("directory entry should be readable")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    eprintln!("[mount_lifecycle] root listing = {:?}", root_listing);
    assert!(root_listing.iter().any(|entry| entry == "movies"));
    assert!(root_listing.iter().any(|entry| entry == "shows"));

    let movies_dir = mountpoint
        .path()
        .join("movies")
        .join("Example Movie (2024)");
    let episode_path = mountpoint
        .path()
        .join("shows")
        .join("Example Show")
        .join("S01")
        .join("E01.mkv");
    eprintln!(
        "[mount_lifecycle] stat movie directory: {}",
        movies_dir.display()
    );
    assert!(std::fs::metadata(&movies_dir)
        .expect("movie directory should stat through FUSE")
        .is_dir());
    eprintln!(
        "[mount_lifecycle] stat episode file: {}",
        episode_path.display()
    );
    assert!(std::fs::metadata(&episode_path)
        .expect("episode file should stat through FUSE")
        .is_file());

    eprintln!("[mount_lifecycle] shutting down mounted session");

    session
        .shutdown()
        .await
        .expect("session shutdown should unmount cleanly");
    eprintln!("[mount_lifecycle] session shutdown completed");
    let _ = shutdown_tx.send(());
    server_task
        .await
        .expect("server join should succeed")
        .expect("mock server should shutdown cleanly");
    eprintln!("[mount_lifecycle] mock server shutdown completed");
}
