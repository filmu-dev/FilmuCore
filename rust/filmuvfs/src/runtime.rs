use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    catalog::state::CatalogStateStore,
    config::SidecarConfig,
    mount::{build_catalog_entry_refresh_client, MountRuntime, Session},
    upstream::UpstreamReader,
};

pub struct SidecarRuntime {
    config: SidecarConfig,
    catalog_state: Arc<CatalogStateStore>,
    mount_runtime: Arc<MountRuntime>,
}

impl SidecarRuntime {
    pub fn new(config: SidecarConfig) -> Result<Self> {
        let catalog_state = Arc::new(CatalogStateStore::new());
        let upstream_reader = UpstreamReader::new();
        let mount_runtime = Arc::new(MountRuntime::with_sidecar_config_and_upstream_reader(
            Arc::clone(&catalog_state),
            config.session_id.clone(),
            &config,
            upstream_reader,
        )?);
        mount_runtime.set_refresh_client(build_catalog_entry_refresh_client(&config));
        Ok(Self {
            config,
            mount_runtime,
            catalog_state,
        })
    }

    pub fn catalog_state(&self) -> Arc<CatalogStateStore> {
        Arc::clone(&self.catalog_state)
    }

    pub fn mount_runtime(&self) -> Arc<MountRuntime> {
        Arc::clone(&self.mount_runtime)
    }

    pub async fn run(self, cancel: CancellationToken) -> Result<()> {
        let session = Session::mount(
            self.config.mountpoint.clone(),
            self.config.grpc_endpoint.clone(),
            self.config.clone(),
            Arc::clone(&self.catalog_state),
            Arc::clone(&self.mount_runtime),
        )
        .await?;

        cancel.cancelled().await;
        info!(
            session_id = %self.config.session_id,
            mountpoint = %self.config.mountpoint.display(),
            "sidecar runtime cancellation requested; shutting down mounted session"
        );
        session.shutdown().await
    }
}
