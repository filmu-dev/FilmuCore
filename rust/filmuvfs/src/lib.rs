//! Rust FilmuVFS sidecar scaffold.
//!
//! This crate now includes the WatchCatalog runtime, a first mount-facing lifecycle layer,
//! a Unix-only `fuse3` trait adapter boundary, the chunk engine read path,
//! and an optional persistent disk-backed cache layer.

pub mod cache;
pub mod capabilities;
pub mod catalog;
pub mod chunk_engine;
pub mod chunk_planner;
pub mod config;
pub mod cross_process_observability;
pub mod hidden_paths;
pub mod media_path;
pub mod mount;
pub mod prefetch;
pub mod proto;
pub mod runtime;
pub mod telemetry;
pub mod upstream;
#[cfg(target_os = "windows")]
pub mod windows_host;
#[cfg(target_os = "windows")]
pub mod windows_projfs;
#[cfg(target_os = "windows")]
pub mod windows_winfsp;

#[cfg(not(target_os = "windows"))]
pub mod windows_winfsp {
    #[must_use]
    pub const fn winfsp_backend_compiled() -> bool {
        false
    }
}

pub const SERVICE_NAME: &str = "filmuvfs";
