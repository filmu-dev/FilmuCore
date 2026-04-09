use std::{
    io::{Error, ErrorKind, Result},
    path::Path,
    sync::Arc,
};

use crate::{
    config::ResolvedMountAdapterKind, mount::MountRuntime,
    windows_projfs::WindowsProjfsMountedFilesystem, windows_winfsp::WindowsWinfspMountedFilesystem,
};

#[derive(Debug)]
pub enum WindowsMountedFilesystem {
    Projfs(WindowsProjfsMountedFilesystem),
    Winfsp(Box<WindowsWinfspMountedFilesystem>),
}

impl WindowsMountedFilesystem {
    pub fn mount_path(&self) -> &Path {
        match self {
            Self::Projfs(filesystem) => filesystem.mount_path(),
            Self::Winfsp(filesystem) => filesystem.mount_path(),
        }
    }

    pub async fn unmount(self) -> Result<()> {
        match self {
            Self::Projfs(filesystem) => filesystem.unmount().await,
            Self::Winfsp(filesystem) => filesystem.unmount().await,
        }
    }
}

impl MountRuntime {
    pub async fn mount_windows_filesystem<P: AsRef<Path>>(
        self: Arc<Self>,
        mount_path: P,
        service_name: &str,
        allow_other: bool,
        adapter: ResolvedMountAdapterKind,
    ) -> Result<WindowsMountedFilesystem> {
        let mount_path = mount_path.as_ref().to_path_buf();
        match adapter {
            ResolvedMountAdapterKind::Projfs => self
                .mount_projfs_filesystem(&mount_path, service_name, allow_other)
                .await
                .map(WindowsMountedFilesystem::Projfs),
            ResolvedMountAdapterKind::Winfsp => self
                .mount_winfsp_filesystem(&mount_path, service_name, allow_other)
                .await
                .map(|filesystem| WindowsMountedFilesystem::Winfsp(Box::new(filesystem))),
            ResolvedMountAdapterKind::Fuse => Err(Error::new(
                ErrorKind::Unsupported,
                "the Linux fuse adapter is not supported on Windows hosts",
            )),
        }
    }
}
