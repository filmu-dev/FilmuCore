use std::{cmp, sync::Arc};

use anyhow::{Context, Result};
use async_stream::stream;
use moka::sync::Cache;
use tokio::{
    sync::{mpsc, oneshot},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Endpoint, Request};
use tracing::{debug, info, info_span, warn, Instrument};

use crate::{
    catalog::state::{
        inode_for_entry_id as hashed_inode_for_entry_id, CatalogStateError, CatalogStateStore,
    },
    config::SidecarConfig,
    mount::MountRuntime,
    proto::{
        filmu::vfs::catalog::v1::filmu_vfs_catalog_service_client::FilmuVfsCatalogServiceClient,
        watch_catalog_event, watch_catalog_request, CatalogAck, CatalogCorrelationKeys,
        CatalogEntry, CatalogEntryKind, CatalogHeartbeat, CatalogProblem, CatalogRemoval,
        CatalogSnapshot, CatalogSubscribe, WatchCatalogEvent, WatchCatalogRequest,
    },
};

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

            match self
                .run_session(cancel.child_token(), &mut initial_sync)
                .await
            {
                Ok(()) => {
                    backoff = self.config.reconnect_backoff_initial;
                }
                Err(error) => {
                    if let Some(sender) = initial_sync.take() {
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

    async fn run_session(
        &self,
        cancel: CancellationToken,
        initial_sync: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        let endpoint = Endpoint::from_shared(self.config.grpc_endpoint.clone())
            .context("failed to parse catalog gRPC endpoint")?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.rpc_timeout)
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

        let response = client
            .watch_catalog(Request::new(outbound_stream))
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

            self.handle_event(&outbound_tx, event, initial_sync).await?;
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
