use anyhow::Result;
use filmuvfs::{
    capabilities::BinaryCapabilities,
    config::SidecarConfig,
    runtime::SidecarRuntime,
    telemetry::{log_windows_projfs_summary, TelemetryGuard},
};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args().any(|arg| arg == "--print-capabilities") {
        println!("{}", BinaryCapabilities::detect().to_json());
        return Ok(());
    }

    let config = SidecarConfig::from_env()?;
    info!(
        allow_other = config.allow_other,
        "starting filmuvfs runtime"
    );
    let runtime = SidecarRuntime::new(config.clone())?;
    let telemetry =
        TelemetryGuard::init(&config, runtime.catalog_state(), runtime.mount_runtime())?;
    let cancel = CancellationToken::new();
    let summary_task = spawn_windows_projfs_summary_task(&config, cancel.clone());

    let runtime_task: JoinHandle<Result<()>> = {
        let cancel = cancel.clone();
        tokio::spawn(async move { runtime.run(cancel).await })
    };
    tokio::pin!(runtime_task);

    let task_result = tokio::select! {
        result = &mut runtime_task => {
            match &result {
                Ok(Ok(())) => info!(
                    daemon_id = %config.daemon_id,
                    session_id = %config.session_id,
                    "sidecar runtime task exited cleanly before a shutdown signal"
                ),
                Ok(Err(error)) => error!(
                    daemon_id = %config.daemon_id,
                    session_id = %config.session_id,
                    error = %error,
                    "sidecar runtime task exited with an error before a shutdown signal"
                ),
                Err(join_error) => error!(
                    daemon_id = %config.daemon_id,
                    session_id = %config.session_id,
                    error = %join_error,
                    "sidecar runtime task join failed before a shutdown signal"
                ),
            }
            result
        },
        signal = wait_for_shutdown_signal() => {
            signal?;
            info!(
                daemon_id = %config.daemon_id,
                session_id = %config.session_id,
                "received shutdown signal; shutting down sidecar runtime"
            );
            cancel.cancel();
            (&mut runtime_task).await
        }
    };

    let outcome = match task_result {
        Ok(result) => result,
        Err(join_error) => Err(join_error.into()),
    };

    cancel.cancel();
    if let Some(summary_task) = summary_task {
        if let Err(join_error) = summary_task.await {
            error!(
                error = %join_error,
                "windows projfs summary task exited unexpectedly"
            );
        }
    }

    log_windows_projfs_summary();

    if let Err(error) = telemetry.shutdown() {
        error!(error = %error, "failed to shutdown OpenTelemetry tracer provider cleanly");
    }

    outcome
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result?;
            Ok(())
        }
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}

fn spawn_windows_projfs_summary_task(
    config: &SidecarConfig,
    cancel: CancellationToken,
) -> Option<JoinHandle<()>> {
    let interval = config.windows_projfs_summary_interval;
    if interval.is_zero() {
        return None;
    }

    info!(
        interval_seconds = interval.as_secs(),
        "starting periodic Windows ProjFS summary task"
    );

    Some(tokio::spawn(async move {
        let mut ticker = time::interval_at(Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    log_windows_projfs_summary();
                }
            }
        }
    }))
}
