use std::{cmp, sync::Arc};

use anyhow::{Context, Result};
use async_stream::stream;
#[cfg(not(target_os = "windows"))]
use bytes::Bytes;
#[cfg(not(target_os = "windows"))]
use http_body_util::{BodyExt, Full};
#[cfg(not(target_os = "windows"))]
use hyper::{header::CONTENT_TYPE, Request as HttpRequest, StatusCode, Uri};
#[cfg(not(target_os = "windows"))]
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
#[cfg(not(target_os = "windows"))]
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use moka::sync::Cache;
use prost::Message;
use tokio::{
    process::Command,
    sync::{mpsc, oneshot},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Endpoint, Request};
use tracing::{debug, info, info_span, warn, Instrument};

#[cfg(not(target_os = "windows"))]
use crate::cross_process_observability::apply_http_observability_headers;
use crate::{
    catalog::state::{
        inode_for_entry_id as hashed_inode_for_entry_id, CatalogStateError, CatalogStateStore,
    },
    config::SidecarConfig,
    cross_process_observability::{
        apply_span_correlation_attributes, apply_tonic_observability_metadata,
        cross_process_request_id, SpanCorrelation,
    },
    mount::MountRuntime,
    proto::{
        filmu::vfs::catalog::v1::filmu_vfs_catalog_service_client::FilmuVfsCatalogServiceClient,
        watch_catalog_event, watch_catalog_request, CatalogAck, CatalogCorrelationKeys,
        CatalogEntry, CatalogEntryKind, CatalogHeartbeat, CatalogProblem, CatalogRemoval,
        CatalogSnapshot, CatalogSubscribe, WatchCatalogEvent, WatchCatalogRequest,
    },
};

const HTTP_POLL_FALLBACK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(not(target_os = "windows"))]
const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

#[derive(Debug, thiserror::Error)]
pub enum CatalogWatchError {
    #[error("received a WatchCatalog event with no payload")]
    MissingPayload,
    #[error("catalog state application failed: {0}")]
    State(#[from] CatalogStateError),
    #[error("remote catalog problem {code}: {message}")]
    RemoteProblem { code: String, message: String },
}

#[derive(Clone)]
struct CatalogHttpFallbackClient {
    api_key: String,
    #[cfg(not(target_os = "windows"))]
    daemon_id: String,
    #[cfg(not(target_os = "windows"))]
    session_id: String,
    rpc_timeout: std::time::Duration,
    #[cfg(target_os = "windows")]
    container_name: String,
    #[cfg(not(target_os = "windows"))]
    endpoint: String,
    #[cfg(not(target_os = "windows"))]
    client: Client<HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>, Full<Bytes>>,
}

impl CatalogHttpFallbackClient {
    fn new(
        _endpoint: String,
        api_key: String,
        daemon_id: String,
        session_id: String,
        rpc_timeout: std::time::Duration,
    ) -> Self {
        #[cfg(not(target_os = "windows"))]
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        #[cfg(not(target_os = "windows"))]
        let client = Client::builder(TokioExecutor::new()).build(connector);
        #[cfg(target_os = "windows")]
        let _ = (&daemon_id, &session_id);
        Self {
            api_key,
            #[cfg(not(target_os = "windows"))]
            daemon_id,
            #[cfg(not(target_os = "windows"))]
            session_id,
            rpc_timeout,
            #[cfg(target_os = "windows")]
            container_name: std::env::var("FILMUVFS_WINDOWS_BACKEND_CONTAINER")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "filmu-python".to_owned()),
            #[cfg(not(target_os = "windows"))]
            endpoint: _endpoint,
            #[cfg(not(target_os = "windows"))]
            client,
        }
    }

    async fn fetch_event(
        &self,
        last_applied_generation_id: Option<&str>,
    ) -> Result<WatchCatalogEvent> {
        #[cfg(target_os = "windows")]
        {
            return self
                .fetch_event_via_docker_exec(last_applied_generation_id)
                .await;
        }

        #[cfg(not(target_os = "windows"))]
        {
            let mut url = format!(
                "{}/internal/vfs/watch-event.pb",
                self.endpoint.trim_end_matches('/')
            );
            if let Some(generation_id) = last_applied_generation_id {
                let normalized = generation_id.trim();
                if !normalized.is_empty() && normalized.chars().all(|ch| ch.is_ascii_digit()) {
                    url.push_str("?last_applied_generation_id=");
                    url.push_str(normalized);
                }
            }

            let uri = url
                .parse::<Uri>()
                .context("failed to parse catalog HTTP fallback endpoint")?;
            let poll_span = info_span!(
                "filmuvfs.catalog.watch_http_poll",
                daemon_id = %self.daemon_id,
                session_id = %self.session_id,
                fallback_target = %self.fallback_target(),
            );
            let request_id = cross_process_request_id(&self.session_id, "watch-event");
            poll_span.in_scope(|| {
                apply_span_correlation_attributes(
                    &poll_span,
                    SpanCorrelation {
                        request_id: Some(request_id.as_str()),
                        daemon_id: Some(self.daemon_id.as_str()),
                        session_id: Some(self.session_id.as_str()),
                        ..SpanCorrelation::default()
                    },
                );
            });
            let request = HttpRequest::builder()
                .method("GET")
                .uri(uri)
                .header("x-filmu-vfs-key", self.api_key.as_str());
            let request = apply_http_observability_headers(
                request,
                &poll_span,
                request_id.as_str(),
                &self.daemon_id,
                &self.session_id,
                &[],
            )
            .header(CONTENT_TYPE, PROTOBUF_CONTENT_TYPE)
            .body(Full::new(Bytes::new()))
            .context("failed to build catalog HTTP fallback request")?;
            let response = timeout(self.rpc_timeout, self.client.request(request))
                .await
                .context("catalog HTTP fallback request timed out")?
                .context("catalog HTTP fallback request failed")?;
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .context("failed to collect catalog HTTP fallback response body")?
                .to_bytes();
            if status != StatusCode::OK {
                let detail = String::from_utf8_lossy(body.as_ref()).trim().to_owned();
                anyhow::bail!("catalog HTTP fallback returned status {status}: {detail}");
            }

            WatchCatalogEvent::decode(body.as_ref())
                .context("failed to decode catalog HTTP fallback protobuf payload")
        }
    }

    #[cfg(target_os = "windows")]
    fn fallback_target(&self) -> String {
        format!("docker exec {}", self.container_name)
    }

    #[cfg(not(target_os = "windows"))]
    fn fallback_target(&self) -> String {
        self.endpoint.clone()
    }

    #[cfg(target_os = "windows")]
    async fn fetch_event_via_docker_exec(
        &self,
        last_applied_generation_id: Option<&str>,
    ) -> Result<WatchCatalogEvent> {
        let mut command = Command::new("docker");
        command
            .arg("exec")
            .arg(&self.container_name)
            .arg("python")
            .arg("-m")
            .arg("filmu_py.tools.vfs_http_bridge")
            .arg("--key")
            .arg(&self.api_key)
            .arg("--timeout-seconds")
            .arg(self.rpc_timeout.as_secs().to_string())
            .arg("watch-event");
        if let Some(generation_id) = last_applied_generation_id {
            let normalized = generation_id.trim();
            if !normalized.is_empty() && normalized.chars().all(|ch| ch.is_ascii_digit()) {
                command.arg("--last-applied-generation-id").arg(normalized);
            }
        }

        let output = timeout(self.rpc_timeout, command.output())
            .await
            .context("catalog docker bridge watch-event timed out")?
            .context("failed to execute catalog docker bridge watch-event command")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            anyhow::bail!(
                "catalog docker bridge watch-event failed with status {}: {}",
                output.status,
                stderr
            );
        }

        WatchCatalogEvent::decode(output.stdout.as_ref())
            .context("failed to decode catalog docker bridge watch-event payload")
    }
}

#[derive(Clone)]
pub struct CatalogWatchRuntime {
    config: SidecarConfig,
    state: Arc<CatalogStateStore>,
    mount_runtime: Option<Arc<MountRuntime>>,
    applied_events: Cache<String, ()>,
}

impl CatalogWatchRuntime {
    pub fn new(
        config: SidecarConfig,
        state: Arc<CatalogStateStore>,
        mount_runtime: Option<Arc<MountRuntime>>,
    ) -> Self {
        Self {
            config,
            state,
            mount_runtime,
            applied_events: Cache::builder()
                .max_capacity(8_192)
                .time_to_live(std::time::Duration::from_secs(600))
                .build(),
        }
    }

    pub async fn run_until_cancelled(
        &self,
        cancel: CancellationToken,
        mut initial_sync: Option<oneshot::Sender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        let mut backoff = self.config.reconnect_backoff_initial;

        loop {
            if cancel.is_cancelled() {
                info!(
                    daemon_id = %self.config.daemon_id,
                    session_id = %self.config.session_id,
                    "catalog runtime cancellation requested"
                );
                return Ok(());
            }

            if self.config.windows_force_docker_bridge {
                if let Some(http_fallback) = self.http_fallback_client() {
                    info!(
                        daemon_id = %self.config.daemon_id,
                        session_id = %self.config.session_id,
                        fallback_target = %http_fallback.fallback_target(),
                        "windows docker bridge transport forced; skipping direct gRPC WatchCatalog session"
                    );
                    match self
                        .run_http_poll_fallback_until_cancelled(
                            cancel.child_token(),
                            &http_fallback,
                            &mut initial_sync,
                        )
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(error) => {
                            if let Some(sender) = initial_sync.take() {
                                let _ = sender.send(Err(error.to_string()));
                            }
                            warn!(
                                daemon_id = %self.config.daemon_id,
                                session_id = %self.config.session_id,
                                error = %error,
                                backoff_seconds = backoff.as_secs(),
                                "windows docker bridge transport failed; retrying after backoff"
                            );
                            tokio::select! {
                                _ = cancel.cancelled() => return Ok(()),
                                _ = sleep(backoff) => {}
                            }
                            backoff = cmp::min(
                                backoff.saturating_mul(2),
                                self.config.reconnect_backoff_max,
                            );
                            continue;
                        }
                    }
                }
            }

            match self
                .run_session(cancel.child_token(), &mut initial_sync)
                .await
            {
                Ok(()) => {
                    backoff = self.config.reconnect_backoff_initial;
                }
                Err(error) => {
                    if let Some(http_fallback) = self.http_fallback_client() {
                        let fallback_target = http_fallback.fallback_target();
                        warn!(
                            daemon_id = %self.config.daemon_id,
                            session_id = %self.config.session_id,
                            grpc_error = %error,
                            fallback_target = %fallback_target,
                            "catalog gRPC watch failed; switching to HTTP polling fallback"
                        );
                        match self
                            .run_http_poll_fallback_until_cancelled(
                                cancel.child_token(),
                                &http_fallback,
                                &mut initial_sync,
                            )
                            .await
                        {
                            Ok(()) => return Ok(()),
                            Err(http_error) => {
                                if let Some(sender) = initial_sync.take() {
                                    let _ = sender.send(Err(http_error.to_string()));
                                }
                                warn!(
                                    daemon_id = %self.config.daemon_id,
                                    session_id = %self.config.session_id,
                                    error = %http_error,
                                    backoff_seconds = backoff.as_secs(),
                                    "catalog HTTP polling fallback failed; retrying after backoff"
                                );
                            }
                        }
                    } else if let Some(sender) = initial_sync.take() {
                        let _ = sender.send(Err(error.to_string()));
                    }

                    warn!(
                        daemon_id = %self.config.daemon_id,
                        session_id = %self.config.session_id,
                        error = %error,
                        backoff_seconds = backoff.as_secs(),
                        "catalog watch session failed; retrying after backoff"
                    );

                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = sleep(backoff) => {}
                    }

                    backoff =
                        cmp::min(backoff.saturating_mul(2), self.config.reconnect_backoff_max);
                }
            }
        }
    }

    fn http_fallback_client(&self) -> Option<CatalogHttpFallbackClient> {
        if !cfg!(target_os = "windows") {
            return None;
        }
        let endpoint = self.config.backend_http_base_url.clone()?;
        let api_key = self.config.backend_api_key.clone()?;
        Some(CatalogHttpFallbackClient::new(
            endpoint,
            api_key,
            self.config.daemon_id.clone(),
            self.config.session_id.clone(),
            self.config.rpc_timeout,
        ))
    }

    async fn run_session(
        &self,
        cancel: CancellationToken,
        initial_sync: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        let endpoint = Endpoint::from_shared(self.config.grpc_endpoint.clone())
            .context("failed to parse catalog gRPC endpoint")?
            .connect_timeout(self.config.connect_timeout)
            .tcp_nodelay(true)
            .tcp_keepalive(Some(self.config.heartbeat_interval))
            .http2_keep_alive_interval(self.config.heartbeat_interval)
            .keep_alive_timeout(self.config.rpc_timeout)
            .keep_alive_while_idle(true)
            .initial_connection_window_size(Some(1024 * 1024))
            .initial_stream_window_size(Some(1024 * 1024));

        let channel = endpoint
            .connect()
            .await
            .context("failed to connect to Python WatchCatalog endpoint")?;
        let mut client = FilmuVfsCatalogServiceClient::new(channel);
        let (outbound_tx, mut outbound_rx) =
            mpsc::channel::<WatchCatalogRequest>(self.config.request_buffer);

        outbound_tx
            .send(self.build_subscribe_request())
            .await
            .context("failed to queue initial WatchCatalog subscribe request")?;

        let heartbeat_cancel = cancel.child_token();
        let heartbeat_tx = outbound_tx.clone();
        let heartbeat_request = self.build_heartbeat_request();
        let heartbeat_interval = self.config.heartbeat_interval;
        let heartbeat_task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(heartbeat_interval);
            loop {
                tokio::select! {
                    _ = heartbeat_cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                    _ = ticker.tick() => {
                        if heartbeat_tx.send(heartbeat_request.clone()).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        });

        let outbound_stream = stream! {
            while let Some(message) = outbound_rx.recv().await {
                yield message;
            }
        };

        let session_span = info_span!(
            "filmuvfs.catalog.watch_session",
            daemon_id = %self.config.daemon_id,
            session_id = %self.config.session_id,
            grpc_endpoint = %self.config.grpc_endpoint,
        );
        let request_id = cross_process_request_id(&self.config.session_id, "watch-catalog");
        let mut request = Request::new(outbound_stream);
        session_span.in_scope(|| {
            apply_span_correlation_attributes(
                &session_span,
                SpanCorrelation {
                    request_id: Some(request_id.as_str()),
                    daemon_id: Some(self.config.daemon_id.as_str()),
                    session_id: Some(self.config.session_id.as_str()),
                    ..SpanCorrelation::default()
                },
            );
            apply_tonic_observability_metadata(
                request.metadata_mut(),
                &session_span,
                request_id.as_str(),
                &self.config.daemon_id,
                &self.config.session_id,
                &[],
            );
        });
        let response = client
            .watch_catalog(request)
            .instrument(session_span.clone())
            .await
            .context("WatchCatalog request failed")?;
        let mut inbound = response.into_inner();

        loop {
            let next_event = tokio::select! {
                _ = cancel.cancelled() => break,
                message = inbound.message() => {
                    message.context("failed to read WatchCatalog event")?
                }
            };

            let Some(event) = next_event else {
                break;
            };

            self.handle_event(&outbound_tx, event, initial_sync)
                .instrument(session_span.clone())
                .await?;
        }

        warn!(
            daemon_id = %self.config.daemon_id,
            session_id = %self.config.session_id,
            "WatchCatalog stream ended; reconnecting without unmounting"
        );

        cancel.cancel();
        heartbeat_task
            .await
            .context("heartbeat task join failed")??;
        Ok(())
    }

    async fn run_http_poll_fallback_until_cancelled(
        &self,
        cancel: CancellationToken,
        client: &CatalogHttpFallbackClient,
        initial_sync: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }

            let event = client
                .fetch_event(self.state.generation_id().as_deref())
                .await?;
            self.handle_http_event(event, initial_sync).await?;

            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = sleep(HTTP_POLL_FALLBACK_INTERVAL) => {}
            }
        }
    }

    async fn handle_http_event(
        &self,
        event: WatchCatalogEvent,
        initial_sync: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        if self.applied_events.contains_key(&event.event_id) {
            return Ok(());
        }

        if let Err(error) = self.apply_event_payload(&event, initial_sync).await {
            if let Some(sender) = initial_sync.take() {
                let _ = sender.send(Err(error.to_string()));
            }
            return Err(error);
        }

        self.applied_events.insert(event.event_id.clone(), ());
        Ok(())
    }

    async fn handle_event(
        &self,
        outbound_tx: &mpsc::Sender<WatchCatalogRequest>,
        event: WatchCatalogEvent,
        initial_sync: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        if self.applied_events.contains_key(&event.event_id) {
            outbound_tx
                .send(self.ack_request_for_event(&event))
                .await
                .context("failed to queue duplicate-event acknowledgement")?;
            return Ok(());
        }

        if let Err(error) = self.apply_event_payload(&event, initial_sync).await {
            if let Some(sender) = initial_sync.take() {
                let _ = sender.send(Err(error.to_string()));
            }
            return Err(error);
        }

        self.applied_events.insert(event.event_id.clone(), ());
        outbound_tx
            .send(self.ack_request_for_event(&event))
            .await
            .context("failed to queue WatchCatalog acknowledgement")?;

        Ok(())
    }

    async fn apply_event_payload(
        &self,
        event: &WatchCatalogEvent,
        initial_sync: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        match event.payload.as_ref() {
            Some(watch_catalog_event::Payload::Snapshot(snapshot)) => {
                let span = info_span!(
                    "filmuvfs_catalog_snapshot_applied",
                    session_id = %self.config.session_id,
                    event_id = %event.event_id,
                    generation_id = %snapshot.generation_id,
                    entry_count = snapshot.entries.len(),
                );

                async {
                    self.state.apply_snapshot(snapshot.clone())?;
                    let counts = self.state.counts();
                    info!(
                        directories = counts.directories,
                        files = counts.files,
                        "catalog snapshot applied"
                    );
                    if let Some(sender) = initial_sync.take() {
                        let _ = sender.send(Ok(()));
                    }
                    Ok(())
                }
                .instrument(span)
                .await
            }
            Some(watch_catalog_event::Payload::Delta(delta)) => {
                let removed_inodes = self.inodes_affected_by_removals(&delta.removals);
                let span = info_span!(
                    "filmuvfs_catalog_delta_applied",
                    session_id = %self.config.session_id,
                    event_id = %event.event_id,
                    generation_id = %delta.generation_id,
                    upsert_count = delta.upserts.len(),
                    removal_count = delta.removals.len(),
                );

                async {
                    self.state.apply_delta(delta.clone())?;
                    let invalidated_handles = self
                        .mount_runtime
                        .as_ref()
                        .map(|runtime| runtime.invalidate_handles_for_inodes(&removed_inodes))
                        .unwrap_or(0);
                    let counts = self.state.counts();
                    info!(
                        directories = counts.directories,
                        files = counts.files,
                        invalidated_handles,
                        "catalog delta applied"
                    );
                    Ok(())
                }
                .instrument(span)
                .await
            }
            Some(watch_catalog_event::Payload::Heartbeat(_)) => {
                debug!(
                    event_id = %event.event_id,
                    session_id = %self.config.session_id,
                    "received WatchCatalog heartbeat"
                );
                Ok(())
            }
            Some(watch_catalog_event::Payload::Problem(problem)) => {
                self.handle_problem(problem, initial_sync)
            }
            None => Err(CatalogWatchError::MissingPayload.into()),
        }
    }

    fn handle_problem(
        &self,
        problem: &CatalogProblem,
        initial_sync: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        warn!(
            code = %problem.code,
            message = %problem.message,
            session_id = %self.config.session_id,
            "received remote catalog problem from Python supplier"
        );
        if let Some(sender) = initial_sync.take() {
            let _ = sender.send(Err(problem.message.clone()));
        }
        Err(CatalogWatchError::RemoteProblem {
            code: problem.code.clone(),
            message: problem.message.clone(),
        }
        .into())
    }

    fn build_subscribe_request(&self) -> WatchCatalogRequest {
        WatchCatalogRequest {
            command: Some(watch_catalog_request::Command::Subscribe(
                CatalogSubscribe {
                    daemon_id: self.config.daemon_id.clone(),
                    daemon_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                    last_applied_generation_id: self.state.generation_id(),
                    want_full_snapshot: true,
                    correlation: Some(self.session_correlation()),
                },
            )),
        }
    }

    fn build_heartbeat_request(&self) -> WatchCatalogRequest {
        WatchCatalogRequest {
            command: Some(watch_catalog_request::Command::Heartbeat(
                CatalogHeartbeat {
                    correlation: Some(self.session_correlation()),
                },
            )),
        }
    }

    fn ack_request_for_event(&self, event: &WatchCatalogEvent) -> WatchCatalogRequest {
        let generation_id = match event.payload.as_ref() {
            Some(watch_catalog_event::Payload::Snapshot(CatalogSnapshot {
                generation_id, ..
            })) => Some(generation_id.clone()),
            Some(watch_catalog_event::Payload::Delta(delta)) => Some(delta.generation_id.clone()),
            _ => None,
        };

        WatchCatalogRequest {
            command: Some(watch_catalog_request::Command::Ack(CatalogAck {
                event_id: event.event_id.clone(),
                generation_id,
                correlation: Some(self.session_correlation()),
            })),
        }
    }

    fn session_correlation(&self) -> CatalogCorrelationKeys {
        CatalogCorrelationKeys {
            session_id: Some(self.config.session_id.clone()),
            ..CatalogCorrelationKeys::default()
        }
    }

    fn inodes_affected_by_removals(&self, removals: &[CatalogRemoval]) -> Vec<u64> {
        self.state
            .entries()
            .into_iter()
            .filter(|entry| {
                removals
                    .iter()
                    .any(|removal| removal_covers_entry(removal, entry))
            })
            .map(|entry| {
                self.state
                    .inode_for_entry_id(&entry.entry_id)
                    .unwrap_or_else(|| hashed_inode_for_entry_id(&entry.entry_id))
            })
            .collect()
    }
}

fn removal_covers_entry(removal: &CatalogRemoval, entry: &CatalogEntry) -> bool {
    if entry.entry_id == removal.entry_id || entry.path == removal.path {
        return true;
    }

    if removal.kind() != CatalogEntryKind::Directory {
        return false;
    }

    if removal.path == "/" {
        return true;
    }

    let removal_prefix = format!("{}/", removal.path.trim_end_matches('/'));
    entry.path.starts_with(&removal_prefix)
}
