use std::{
    fs::OpenOptions,
    io::{Error, ErrorKind, Result},
    path::{Path, PathBuf},
    ptr::null_mut,
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::runtime::Handle;
use tracing::{debug, info, warn};
use windows_sys::Win32::Foundation::{
    EXCEPTION_NONCONTINUABLE_EXCEPTION, STATUS_BUFFER_OVERFLOW, STATUS_CANCELLED, STATUS_REPARSE,
    STATUS_SUCCESS,
};
use windows_sys::Win32::System::Console::{GetStdHandle, STD_ERROR_HANDLE};
use winfsp_wrs::{
    filetime_now, init, CreateFileInfo, CreateOptions, DirInfo as WinfspDirInfo, FileAccessRights,
    FileAttributes, FileInfo as WinfspFileInfo, FileSystem, FileSystemInterface,
    PSecurityDescriptor, Params as WinfspParams, SecurityDescriptor, U16CStr, U16CString,
    VolumeInfo,
};
use winfsp_wrs_sys::{
    FspDebugLogSetHandle, FspFileSystemAddDirInfo, FspFileSystemCreate, FspFileSystemDelete,
    FspFileSystemRemoveMountPoint, FspFileSystemSetDebugLogF, FspFileSystemSetMountPoint,
    FspFileSystemSetOperationGuardStrategyF, FspFileSystemStartDispatcher,
    FspFileSystemStopDispatcher, FSP_FILE_SYSTEM, FSP_FILE_SYSTEM_INTERFACE,
    FSP_FILE_SYSTEM_OPERATION_GUARD_STRATEGY_FSP_FILE_SYSTEM_OPERATION_GUARD_STRATEGY_FINE,
    FSP_FSCTL_DIR_INFO, FSP_FSCTL_FILE_INFO, FSP_FSCTL_OPEN_FILE_INFO, FSP_FSCTL_VOLUME_INFO,
    FSP_FSCTL_VOLUME_PARAMS, NTSTATUS, PFILE_FULL_EA_INFORMATION, PSECURITY_DESCRIPTOR, PUINT32,
    PULONG, PVOID, PWSTR, SIZE_T, UINT32, UINT64, ULONG,
};

use crate::mount::{MountNodeKind, MountRuntime, MountRuntimeError};

const STATUS_INVALID_PARAMETER: NTSTATUS = 0xC000_000D_u32 as i32;
const STATUS_ACCESS_DENIED: NTSTATUS = 0xC000_0022_u32 as i32;
const STATUS_OBJECT_NAME_NOT_FOUND: NTSTATUS = 0xC000_0034_u32 as i32;
const STATUS_FILE_IS_A_DIRECTORY: NTSTATUS = 0xC000_00BA_u32 as i32;
const STATUS_NOT_A_DIRECTORY: NTSTATUS = 0xC000_0103_u32 as i32;
const STATUS_END_OF_FILE: NTSTATUS = 0xC000_0011_u32 as i32;
const STATUS_IO_DEVICE_ERROR: NTSTATUS = 0xC000_0185_u32 as i32;
const STATUS_DEVICE_NOT_READY: NTSTATUS = 0xC000_00A3_u32 as i32;

const DEFAULT_VOLUME_SIZE_BYTES: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const DEFAULT_VOLUME_FREE_BYTES: u64 = 0;
const DEFAULT_SECURITY_DESCRIPTOR: &str =
    "O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FR;;;BU)(A;;FR;;;WD)";
const FILE_SYSTEM_NAME: &str = "FILMUVFS";
const WINFSP_DISK_DEVICE_NAME: &str = "WinFsp.Disk";

enum WinfspBackendHandle {
    Wrapper(Box<FileSystem>),
    Raw {
        file_system: *mut FSP_FILE_SYSTEM,
        context: *mut WinfspFilesystem,
        interface: *mut FSP_FILE_SYSTEM_INTERFACE,
    },
}

pub struct WindowsWinfspMountedFilesystem {
    mount_path: PathBuf,
    backend: Option<WinfspBackendHandle>,
}

unsafe impl Send for WindowsWinfspMountedFilesystem {}

impl std::fmt::Debug for WindowsWinfspMountedFilesystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowsWinfspMountedFilesystem")
            .field("mount_path", &self.mount_path)
            .finish_non_exhaustive()
    }
}

impl WindowsWinfspMountedFilesystem {
    pub fn mount_path(&self) -> &Path {
        &self.mount_path
    }

    pub async fn unmount(mut self) -> Result<()> {
        info!(mount_path = %self.mount_path.display(), "stopping WinFSP mount");
        if let Some(backend) = self.backend.take() {
            match backend {
                WinfspBackendHandle::Wrapper(file_system) => {
                    file_system.stop();
                }
                WinfspBackendHandle::Raw {
                    file_system,
                    context,
                    interface,
                } => unsafe {
                    FspFileSystemStopDispatcher(file_system);
                    FspFileSystemRemoveMountPoint(file_system);
                    FspFileSystemDelete(file_system);
                    drop(Box::from_raw(context));
                    drop(Box::from_raw(interface));
                },
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct WinfspFileContext {
    inode: u64,
    path: String,
    kind: MountNodeKind,
    size_bytes: u64,
    handle_id: Option<u64>,
}

#[derive(Clone)]
struct WinfspFilesystem {
    runtime: Arc<MountRuntime>,
    runtime_handle: Handle,
    security_descriptor: Arc<SecurityDescriptor>,
    volume_label: Arc<U16CString>,
}

impl WinfspFilesystem {
    fn new(
        runtime: Arc<MountRuntime>,
        runtime_handle: Handle,
        security_descriptor: Arc<SecurityDescriptor>,
        volume_label: Arc<U16CString>,
    ) -> Self {
        Self {
            runtime,
            runtime_handle,
            security_descriptor,
            volume_label,
        }
    }

    fn normalized_path(file_name: &U16CStr) -> String {
        let normalized = file_name.to_string_lossy().replace('\\', "/");
        let trimmed = strip_stream_suffix(normalized.trim_matches('/'));
        if trimmed.is_empty() {
            "/".to_owned()
        } else {
            format!("/{}", trimmed)
        }
    }

    fn volume_info(&self) -> FSP_FSCTL_VOLUME_INFO {
        let mut info = FSP_FSCTL_VOLUME_INFO {
            TotalSize: DEFAULT_VOLUME_SIZE_BYTES,
            FreeSize: DEFAULT_VOLUME_FREE_BYTES,
            VolumeLabelLength: 0,
            VolumeLabel: [0; 32],
        };
        let label = self.volume_label.as_slice();
        let copy_len = label.len().min(info.VolumeLabel.len());
        info.VolumeLabel[..copy_len].copy_from_slice(&label[..copy_len]);
        info.VolumeLabelLength = (copy_len * std::mem::size_of::<u16>()) as u16;
        info
    }

    fn winfsp_volume_info(&self) -> std::result::Result<VolumeInfo, NTSTATUS> {
        VolumeInfo::new(
            DEFAULT_VOLUME_SIZE_BYTES,
            DEFAULT_VOLUME_FREE_BYTES,
            self.volume_label.as_ustr(),
        )
        .map_err(|_| STATUS_INVALID_PARAMETER)
    }

    fn mount_file_info(&self, inode: u64) -> std::result::Result<FSP_FSCTL_FILE_INFO, NTSTATUS> {
        let attributes = self
            .runtime
            .getattr_by_inode(inode)
            .map_err(ntstatus_from_mount_error)?;
        Ok(raw_file_info_from_attributes(&attributes))
    }

    fn winfsp_file_info_from_raw(file_info: FSP_FSCTL_FILE_INFO) -> WinfspFileInfo {
        let mut info = WinfspFileInfo::default();
        info.set_file_attributes(FileAttributes(file_info.FileAttributes))
            .set_reparse_tag(file_info.ReparseTag)
            .set_allocation_size(file_info.AllocationSize)
            .set_file_size(file_info.FileSize)
            .set_creation_time(file_info.CreationTime)
            .set_last_access_time(file_info.LastAccessTime)
            .set_last_write_time(file_info.LastWriteTime)
            .set_change_time(file_info.ChangeTime)
            .set_index_number(file_info.IndexNumber)
            .set_hard_links(file_info.HardLinks)
            .set_ea_size(file_info.EaSize);
        info
    }

    fn winfsp_file_info_from_attributes(
        attributes: &crate::mount::MountAttributes,
    ) -> WinfspFileInfo {
        Self::winfsp_file_info_from_raw(raw_file_info_from_attributes(attributes))
    }

    fn context_file_info(
        &self,
        file_context: &WinfspFileContext,
    ) -> std::result::Result<FSP_FSCTL_FILE_INFO, NTSTATUS> {
        self.mount_file_info(file_context.inode)
    }

    fn open_directory_context(
        &self,
        path: String,
    ) -> std::result::Result<(WinfspFileContext, FSP_FSCTL_FILE_INFO), NTSTATUS> {
        let attributes = self
            .runtime
            .getattr(&path)
            .map_err(ntstatus_from_mount_error)?;
        let file_info = raw_file_info_from_attributes(&attributes);
        Ok((
            WinfspFileContext {
                inode: attributes.inode,
                path,
                kind: MountNodeKind::Directory,
                size_bytes: attributes.size_bytes,
                handle_id: None,
            },
            file_info,
        ))
    }

    fn open_file_context(
        &self,
        path: String,
    ) -> std::result::Result<(WinfspFileContext, FSP_FSCTL_FILE_INFO), NTSTATUS> {
        let handle = self
            .runtime
            .open(&path)
            .map_err(ntstatus_from_mount_error)?;
        self.runtime
            .spawn_startup_prefetch(&handle, &self.runtime_handle);
        let file_info = self.mount_file_info(handle.inode).inspect_err(|_| {
            let _ = self.runtime.release(handle.handle_id);
        })?;
        Ok((
            WinfspFileContext {
                inode: handle.inode,
                path,
                kind: MountNodeKind::File,
                size_bytes: handle.size_bytes.unwrap_or(0),
                handle_id: Some(handle.handle_id),
            },
            file_info,
        ))
    }

    fn sorted_directory_entries(
        &self,
        directory_inode: u64,
    ) -> std::result::Result<Vec<crate::mount::MountDirectoryEntry>, NTSTATUS> {
        let mut entries = self
            .runtime
            .readdir_by_inode(directory_inode)
            .map_err(ntstatus_from_mount_error)?;
        entries.retain(|entry| entry.name != "." && entry.name != "..");
        entries.sort_by(|left, right| {
            left.name
                .to_ascii_lowercase()
                .cmp(&right.name.to_ascii_lowercase())
                .then_with(|| left.name.cmp(&right.name))
        });
        Ok(entries)
    }

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
    ) -> std::result::Result<(u32, bool), NTSTATUS> {
        let normalized_path = Self::normalized_path(file_name);
        info!(path = %normalized_path, "winfsp.get_security_by_name");
        let attributes = self
            .runtime
            .getattr(&normalized_path)
            .map_err(ntstatus_from_mount_error)?;
        Ok((file_attributes_from_kind(attributes.kind).0, false))
    }

    fn create_ex(
        &self,
        file_name: &U16CStr,
        create_options: UINT32,
        granted_access: UINT32,
        file_attributes: UINT32,
        allocation_size: UINT64,
        extra_buffer_is_reparse_point: bool,
    ) -> std::result::Result<(WinfspFileContext, FSP_FSCTL_FILE_INFO), NTSTATUS> {
        let normalized_path = Self::normalized_path(file_name);
        info!(
            path = %normalized_path,
            create_options,
            granted_access,
            file_attributes,
            allocation_size,
            extra_buffer_is_reparse_point,
            "winfsp.create_ex"
        );
        self.open(file_name, create_options, granted_access)
    }

    fn create(
        &self,
        file_name: &U16CStr,
        create_options: UINT32,
        granted_access: UINT32,
        file_attributes: UINT32,
        allocation_size: UINT64,
    ) -> std::result::Result<(WinfspFileContext, FSP_FSCTL_FILE_INFO), NTSTATUS> {
        let normalized_path = Self::normalized_path(file_name);
        info!(
            path = %normalized_path,
            create_options,
            granted_access,
            file_attributes,
            allocation_size,
            "winfsp.create"
        );
        self.open(file_name, create_options, granted_access)
    }

    fn open(
        &self,
        file_name: &U16CStr,
        create_options: UINT32,
        granted_access: UINT32,
    ) -> std::result::Result<(WinfspFileContext, FSP_FSCTL_FILE_INFO), NTSTATUS> {
        let normalized_path = Self::normalized_path(file_name);
        info!(
            path = %normalized_path,
            create_options,
            granted_access,
            "winfsp.open"
        );
        let attributes = match self.runtime.getattr(&normalized_path) {
            Ok(attributes) => attributes,
            Err(error) => {
                let status = ntstatus_from_mount_error(error);
                warn!(
                    path = %normalized_path,
                    status = format_args!("{:#010x}", status as u32),
                    "winfsp.open getattr failed"
                );
                return Err(status);
            }
        };

        let result = match attributes.kind {
            MountNodeKind::Directory => self.open_directory_context(normalized_path.clone()),
            MountNodeKind::File => self.open_file_context(normalized_path.clone()),
        };

        match &result {
            Ok(_) => info!(path = %normalized_path, "winfsp.open -> ok"),
            Err(status) => warn!(
                path = %normalized_path,
                status = format_args!("{:#010x}", *status as u32),
                "winfsp.open -> err"
            ),
        }

        result
    }

    fn close(&self, file_context: &WinfspFileContext) {
        if let Some(handle_id) = file_context.handle_id {
            if let Err(error) = self.runtime.release(handle_id) {
                warn!(
                    handle_id,
                    inode = file_context.inode,
                    context_ptr = format_args!("{:#x}", file_context_ptr_ref(file_context)),
                    ?error,
                    "winfsp.close failed to release mount handle"
                );
            }
        }
    }

    fn read(
        &self,
        file_context: &WinfspFileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> std::result::Result<usize, NTSTATUS> {
        let Some(handle_id) = file_context.handle_id else {
            return Err(STATUS_FILE_IS_A_DIRECTORY);
        };
        let requested_len = buffer.len();
        info!(
            inode = file_context.inode,
            handle_id,
            offset,
            requested_len,
            file_size = file_context.size_bytes,
            context_ptr = format_args!("{:#x}", file_context_ptr_ref(file_context)),
            "winfsp.read.guard"
        );
        if requested_len == 0 {
            warn!(
                inode = file_context.inode,
                handle_id, "winfsp.read zero-length request"
            );
            return Ok(0);
        }
        if file_context.size_bytes > 0 && offset >= file_context.size_bytes {
            warn!(
                inode = file_context.inode,
                handle_id,
                offset,
                file_size = file_context.size_bytes,
                "winfsp.read offset past eof"
            );
            return Ok(0);
        }
        if requested_len > 32 * 1024 * 1024 {
            warn!(
                inode = file_context.inode,
                handle_id, offset, requested_len, "winfsp.read clamping large request"
            );
        }
        let effective_len = if file_context.size_bytes > 0 {
            let remaining = file_context.size_bytes.saturating_sub(offset);
            requested_len.min(remaining.min(32 * 1024 * 1024) as usize)
        } else {
            requested_len.min(32 * 1024 * 1024)
        };
        if effective_len == 0 {
            return Ok(0);
        }
        debug!(
            inode = file_context.inode,
            handle_id,
            offset,
            requested = requested_len,
            effective_len,
            context_ptr = format_args!("{:#x}", file_context_ptr_ref(file_context)),
            "winfsp.read"
        );
        let bytes = match self.runtime_handle.block_on(self.runtime.read_bytes(
            handle_id,
            file_context.inode,
            offset,
            effective_len.min(u32::MAX as usize) as u32,
        )) {
            Ok(bytes) => bytes,
            Err(MountRuntimeError::ReadAborted { .. }) => return Ok(0),
            Err(error) => return Err(ntstatus_from_mount_error(error)),
        };
        if bytes.is_empty() && !buffer.is_empty() && offset > 0 {
            return Err(STATUS_END_OF_FILE);
        }
        let len = bytes.len();
        buffer[..len].copy_from_slice(&bytes);
        Ok(len)
    }

    fn flush(
        &self,
        file_context: Option<&WinfspFileContext>,
    ) -> std::result::Result<FSP_FSCTL_FILE_INFO, NTSTATUS> {
        match file_context {
            Some(file_context) => self.context_file_info(file_context),
            None => Ok(raw_file_info_for_volume()),
        }
    }

    fn get_file_info(
        &self,
        file_context: &WinfspFileContext,
    ) -> std::result::Result<FSP_FSCTL_FILE_INFO, NTSTATUS> {
        self.context_file_info(file_context)
    }

    fn read_directory(
        &self,
        file_context: &WinfspFileContext,
        marker: Option<&U16CStr>,
        buffer: PVOID,
        buffer_len: ULONG,
        bytes_transferred: PULONG,
    ) -> std::result::Result<(), NTSTATUS> {
        if file_context.kind != MountNodeKind::Directory {
            return Err(STATUS_NOT_A_DIRECTORY);
        }

        let marker = marker.map(|value| value.to_string_lossy());
        let marker_lower = marker.as_ref().map(|value| value.to_ascii_lowercase());
        let entries = self.sorted_directory_entries(file_context.inode)?;
        info!(
            path = %file_context.path,
            inode = file_context.inode,
            marker = ?marker,
            entry_count = entries.len(),
            "winfsp.read_directory"
        );

        unsafe {
            if !bytes_transferred.is_null() {
                bytes_transferred.write(0);
            }
        }

        for entry in entries {
            if let Some(marker_lower) = marker_lower.as_ref() {
                let candidate_lower = entry.name.to_ascii_lowercase();
                if candidate_lower <= *marker_lower {
                    continue;
                }
            }

            let file_info = self.mount_file_info(entry.inode)?;
            let mut dir_info = dir_info_from_entry(file_info, &entry.name);
            let added = unsafe {
                FspFileSystemAddDirInfo(
                    (&mut dir_info as *mut DirInfo).cast::<FSP_FSCTL_DIR_INFO>(),
                    buffer,
                    buffer_len,
                    bytes_transferred,
                )
            };
            if added == 0 {
                break;
            }
        }

        unsafe {
            FspFileSystemAddDirInfo(null_mut(), buffer, buffer_len, bytes_transferred);
        }
        Ok(())
    }

    fn get_dir_info_by_name(
        &self,
        file_context: &WinfspFileContext,
        file_name: &U16CStr,
        dir_info: *mut FSP_FSCTL_DIR_INFO,
    ) -> std::result::Result<(), NTSTATUS> {
        if file_context.kind != MountNodeKind::Directory {
            return Err(STATUS_NOT_A_DIRECTORY);
        }

        let name = file_name.to_string_lossy();
        info!(
            directory = %file_context.path,
            name = %name,
            "winfsp.get_dir_info_by_name"
        );
        let child_path = if file_context.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", file_context.path, name)
        };

        let attributes = self
            .runtime
            .getattr(&child_path)
            .map_err(ntstatus_from_mount_error)?;
        let info = dir_info_from_entry(raw_file_info_from_attributes(&attributes), &name);
        unsafe {
            std::ptr::copy_nonoverlapping(
                (&info as *const DirInfo).cast::<u8>(),
                dir_info.cast::<u8>(),
                info.size as usize,
            );
        }
        Ok(())
    }
}

impl FileSystemInterface for WinfspFilesystem {
    type FileContext = Arc<WinfspFileContext>;

    const GET_VOLUME_INFO_DEFINED: bool = true;
    const GET_SECURITY_BY_NAME_DEFINED: bool = true;
    const CREATE_DEFINED: bool = true;
    const CREATE_EX_DEFINED: bool = true;
    const OPEN_DEFINED: bool = true;
    const CLOSE_DEFINED: bool = true;
    const READ_DEFINED: bool = true;
    const FLUSH_DEFINED: bool = true;
    const GET_FILE_INFO_DEFINED: bool = true;
    const GET_SECURITY_DEFINED: bool = true;
    const READ_DIRECTORY_DEFINED: bool = true;
    const GET_DIR_INFO_BY_NAME_DEFINED: bool = true;

    fn get_volume_info(&self) -> std::result::Result<VolumeInfo, NTSTATUS> {
        info!("winfsp.get_volume_info");
        trace_callback("get_volume_info");
        self.winfsp_volume_info()
    }

    fn get_security_by_name(
        &self,
        file_name: &U16CStr,
        _find_reparse_point: impl Fn() -> Option<FileAttributes>,
    ) -> std::result::Result<(FileAttributes, PSecurityDescriptor, bool), NTSTATUS> {
        let path = Self::normalized_path(file_name);
        trace_callback(format!("trait.get_security_by_name path={path}"));
        match WinfspFilesystem::get_security_by_name(self, file_name) {
            Ok((attributes, reparse)) => {
                trace_callback(format!(
                    "trait.get_security_by_name status=success path={path} attrs=0x{:08X} reparse={reparse}",
                    attributes
                ));
                Ok((
                    FileAttributes(attributes),
                    self.security_descriptor.as_ref().as_ptr(),
                    reparse,
                ))
            }
            Err(status) => {
                trace_callback(format!(
                    "trait.get_security_by_name status=0x{:08X} path={path}",
                    status as u32
                ));
                Err(status)
            }
        }
    }

    fn create(
        &self,
        file_name: &U16CStr,
        create_file_info: CreateFileInfo,
        _security_descriptor: SecurityDescriptor,
    ) -> std::result::Result<(Self::FileContext, WinfspFileInfo), NTSTATUS> {
        let path = Self::normalized_path(file_name);
        trace_callback(format!(
            "trait.create path={path} create_options=0x{:08X} granted_access=0x{:08X}",
            create_file_info.create_options.0, create_file_info.granted_access.0
        ));
        let (context, file_info) = WinfspFilesystem::create(
            self,
            file_name,
            create_file_info.create_options.0,
            create_file_info.granted_access.0,
            create_file_info.file_attributes.0,
            create_file_info.allocation_size,
        )?;
        trace_callback(format!("trait.create status=success path={path}"));
        Ok((
            Arc::new(context),
            Self::winfsp_file_info_from_raw(file_info),
        ))
    }

    fn create_ex(
        &self,
        file_name: &U16CStr,
        create_file_info: CreateFileInfo,
        _security_descriptor: SecurityDescriptor,
        _buffer: &[u8],
        extra_buffer_is_reparse_point: bool,
    ) -> std::result::Result<(Self::FileContext, WinfspFileInfo), NTSTATUS> {
        let path = Self::normalized_path(file_name);
        trace_callback(format!(
            "trait.create_ex path={path} create_options=0x{:08X} granted_access=0x{:08X} reparse={extra_buffer_is_reparse_point}",
            create_file_info.create_options.0,
            create_file_info.granted_access.0
        ));
        let (context, file_info) = WinfspFilesystem::create_ex(
            self,
            file_name,
            create_file_info.create_options.0,
            create_file_info.granted_access.0,
            create_file_info.file_attributes.0,
            create_file_info.allocation_size,
            extra_buffer_is_reparse_point,
        )?;
        trace_callback(format!("trait.create_ex status=success path={path}"));
        Ok((
            Arc::new(context),
            Self::winfsp_file_info_from_raw(file_info),
        ))
    }

    fn open(
        &self,
        file_name: &U16CStr,
        create_options: CreateOptions,
        granted_access: FileAccessRights,
    ) -> std::result::Result<(Self::FileContext, WinfspFileInfo), NTSTATUS> {
        let path = Self::normalized_path(file_name);
        trace_callback(format!(
            "trait.open path={path} create_options=0x{:08X} granted_access=0x{:08X}",
            create_options.0, granted_access.0
        ));
        let (context, file_info) =
            WinfspFilesystem::open(self, file_name, create_options.0, granted_access.0)?;
        trace_callback(format!("trait.open status=success path={path}"));
        Ok((
            Arc::new(context),
            Self::winfsp_file_info_from_raw(file_info),
        ))
    }

    fn close(&self, file_context: Self::FileContext) {
        trace_callback(format!("trait.close path={}", file_context.path));
        WinfspFilesystem::close(self, &file_context);
    }

    fn read(
        &self,
        file_context: Self::FileContext,
        buffer: &mut [u8],
        offset: u64,
    ) -> std::result::Result<usize, NTSTATUS> {
        trace_callback(format!(
            "trait.read path={} offset={} length={}",
            file_context.path,
            offset,
            buffer.len()
        ));
        let read = WinfspFilesystem::read(self, &file_context, buffer, offset)?;
        trace_callback(format!(
            "trait.read status=success path={} bytes={}",
            file_context.path, read
        ));
        Ok(read)
    }

    fn flush(
        &self,
        file_context: Self::FileContext,
    ) -> std::result::Result<WinfspFileInfo, NTSTATUS> {
        trace_callback(format!("trait.flush path={}", file_context.path));
        let info = WinfspFilesystem::flush(self, Some(&file_context))?;
        Ok(Self::winfsp_file_info_from_raw(info))
    }

    fn get_file_info(
        &self,
        file_context: Self::FileContext,
    ) -> std::result::Result<WinfspFileInfo, NTSTATUS> {
        trace_callback(format!("trait.get_file_info path={}", file_context.path));
        let info = WinfspFilesystem::get_file_info(self, &file_context)?;
        Ok(Self::winfsp_file_info_from_raw(info))
    }

    fn get_security(
        &self,
        _file_context: Self::FileContext,
    ) -> std::result::Result<PSecurityDescriptor, NTSTATUS> {
        trace_callback("trait.get_security");
        Ok(self.security_descriptor.as_ref().as_ptr())
    }

    fn read_directory(
        &self,
        file_context: Self::FileContext,
        marker: Option<&U16CStr>,
        mut add_dir_info: impl FnMut(WinfspDirInfo) -> bool,
    ) -> std::result::Result<(), NTSTATUS> {
        trace_callback(format!(
            "trait.read_directory path={} marker={}",
            file_context.path,
            marker
                .map(|value| value.to_string_lossy())
                .unwrap_or_else(|| "<none>".to_owned())
        ));
        if file_context.kind != MountNodeKind::Directory {
            return Err(STATUS_NOT_A_DIRECTORY);
        }

        let marker = marker.map(|value| value.to_string_lossy());
        let marker_lower = marker.as_ref().map(|value| value.to_ascii_lowercase());
        let entries = self.sorted_directory_entries(file_context.inode)?;
        info!(
            path = %file_context.path,
            inode = file_context.inode,
            marker = ?marker,
            entry_count = entries.len(),
            "winfsp.read_directory"
        );

        for entry in entries {
            if let Some(marker_lower) = marker_lower.as_ref() {
                let candidate_lower = entry.name.to_ascii_lowercase();
                if candidate_lower <= *marker_lower {
                    continue;
                }
            }

            let file_info = self.mount_file_info(entry.inode)?;
            if !add_dir_info(WinfspDirInfo::from_str(
                Self::winfsp_file_info_from_raw(file_info),
                &entry.name,
            )) {
                break;
            }
        }

        Ok(())
    }

    fn get_dir_info_by_name(
        &self,
        file_context: Self::FileContext,
        file_name: &U16CStr,
    ) -> std::result::Result<WinfspFileInfo, NTSTATUS> {
        trace_callback(format!(
            "trait.get_dir_info_by_name parent={} child={}",
            file_context.path,
            file_name.to_string_lossy()
        ));
        if file_context.kind != MountNodeKind::Directory {
            return Err(STATUS_NOT_A_DIRECTORY);
        }

        let name = file_name.to_string_lossy();
        info!(
            directory = %file_context.path,
            name = %name,
            "winfsp.get_dir_info_by_name"
        );
        let child_path = if file_context.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", file_context.path, name)
        };

        let attributes = self
            .runtime
            .getattr(&child_path)
            .map_err(ntstatus_from_mount_error)?;
        Ok(Self::winfsp_file_info_from_attributes(&attributes))
    }
}

macro_rules! catch_panic_status {
    ($body:block) => {
        std::panic::catch_unwind(|| $body).unwrap_or(EXCEPTION_NONCONTINUABLE_EXCEPTION)
    };
}

unsafe fn fs_context<'a>(file_system: *mut FSP_FILE_SYSTEM) -> &'a WinfspFilesystem {
    &*((*file_system).UserContext.cast::<WinfspFilesystem>())
}

unsafe fn borrow_file_context<'a>(raw: PVOID) -> &'a WinfspFileContext {
    &*raw.cast::<WinfspFileContext>()
}

unsafe fn take_file_context(raw: PVOID) -> Box<WinfspFileContext> {
    Box::from_raw(raw.cast::<WinfspFileContext>())
}

unsafe fn write_file_context(file_context: WinfspFileContext, out: *mut PVOID) {
    let raw = Box::into_raw(Box::new(file_context));
    out.write(raw.cast());
}

fn file_context_ptr_ref(file_context: &WinfspFileContext) -> usize {
    file_context as *const WinfspFileContext as usize
}

fn callback_trace_path() -> Option<&'static PathBuf> {
    static TRACE_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();
    TRACE_PATH
        .get_or_init(|| std::env::var_os("FILMUVFS_WINDOWS_TRACE_PATH").map(PathBuf::from))
        .as_ref()
}

fn trace_callback(message: impl AsRef<str>) {
    let Some(path) = callback_trace_path() else {
        return;
    };
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
        return;
    };
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    let _ = std::io::Write::write_all(
        &mut file,
        format!("{timestamp} {}\n", message.as_ref()).as_bytes(),
    );
}

fn copy_security_descriptor(
    descriptor: &SecurityDescriptor,
    out_descriptor: PSECURITY_DESCRIPTOR,
    out_descriptor_size: *mut SIZE_T,
) -> std::result::Result<(), NTSTATUS> {
    let descriptor_bytes = descriptor.as_slice();
    let descriptor_len = descriptor_bytes.len() as SIZE_T;

    unsafe {
        if out_descriptor_size.is_null() {
            return if descriptor_len == 0 {
                Ok(())
            } else {
                Err(STATUS_INVALID_PARAMETER)
            };
        }

        let available_len = out_descriptor_size.read();
        if out_descriptor.is_null() || descriptor_len > available_len {
            out_descriptor_size.write(descriptor_len);
            return Err(STATUS_BUFFER_OVERFLOW);
        }

        std::ptr::copy_nonoverlapping(
            descriptor_bytes.as_ptr(),
            out_descriptor.cast::<u8>(),
            descriptor_bytes.len(),
        );
        out_descriptor_size.write(descriptor_len);
    }

    Ok(())
}

unsafe extern "C" fn winfsp_get_volume_info(
    file_system: *mut FSP_FILE_SYSTEM,
    volume_info: *mut FSP_FSCTL_VOLUME_INFO,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null() || volume_info.is_null() {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        info!("winfsp.get_volume_info");
        trace_callback("get_volume_info");
        unsafe {
            volume_info.write(fs.volume_info());
        }
        STATUS_SUCCESS
    })
}

unsafe extern "C" fn winfsp_set_volume_label(
    file_system: *mut FSP_FILE_SYSTEM,
    _volume_label: PWSTR,
    volume_info: *mut FSP_FSCTL_VOLUME_INFO,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null() || volume_info.is_null() {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        trace_callback("set_volume_label");
        unsafe {
            volume_info.write(fs.volume_info());
        }
        STATUS_SUCCESS
    })
}

unsafe extern "C" fn winfsp_get_security_by_name(
    file_system: *mut FSP_FILE_SYSTEM,
    file_name: PWSTR,
    file_attributes: PUINT32,
    security_descriptor: PSECURITY_DESCRIPTOR,
    security_descriptor_size: *mut SIZE_T,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null() || file_name.is_null() {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        let file_name = unsafe { U16CStr::from_ptr_str(file_name) };
        trace_callback(format!(
            "get_security_by_name path={}",
            WinfspFilesystem::normalized_path(file_name)
        ));
        match fs.get_security_by_name(file_name) {
            Ok((attributes, reparse)) => {
                if !file_attributes.is_null() {
                    unsafe {
                        file_attributes.write(attributes);
                    }
                }
                if let Err(status) = copy_security_descriptor(
                    fs.security_descriptor.as_ref(),
                    security_descriptor,
                    security_descriptor_size,
                ) {
                    return status;
                }
                if reparse {
                    STATUS_REPARSE
                } else {
                    STATUS_SUCCESS
                }
            }
            Err(status) => status,
        }
    })
}

#[expect(
    dead_code,
    reason = "CreateEx is kept as the planned extended WinFSP callback, but the interface table still uses Create only"
)]
unsafe extern "C" fn winfsp_create_ex(
    file_system: *mut FSP_FILE_SYSTEM,
    file_name: PWSTR,
    create_options: UINT32,
    granted_access: UINT32,
    file_attributes: UINT32,
    _security_descriptor: PSECURITY_DESCRIPTOR,
    allocation_size: UINT64,
    _extra_buffer: PVOID,
    _extra_length: ULONG,
    extra_buffer_is_reparse_point: u8,
    file_context: *mut PVOID,
    file_info: *mut FSP_FSCTL_FILE_INFO,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null()
            || file_name.is_null()
            || file_context.is_null()
            || file_info.is_null()
        {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        let file_name = unsafe { U16CStr::from_ptr_str(file_name) };
        trace_callback(format!(
            "create_ex path={} create_options={} granted_access={}",
            WinfspFilesystem::normalized_path(file_name),
            create_options,
            granted_access
        ));
        match fs.create_ex(
            file_name,
            create_options,
            granted_access,
            file_attributes,
            allocation_size,
            extra_buffer_is_reparse_point != 0,
        ) {
            Ok((context, info)) => {
                unsafe {
                    write_file_context(context, file_context);
                    (*file_info.cast::<FSP_FSCTL_OPEN_FILE_INFO>()).FileInfo = info;
                }
                trace_callback("create_ex status=success");
                STATUS_SUCCESS
            }
            Err(status) => {
                trace_callback(format!("create_ex status=0x{:08X}", status as u32));
                status
            }
        }
    })
}

unsafe extern "C" fn winfsp_create(
    file_system: *mut FSP_FILE_SYSTEM,
    file_name: PWSTR,
    create_options: UINT32,
    granted_access: UINT32,
    file_attributes: UINT32,
    _security_descriptor: PSECURITY_DESCRIPTOR,
    allocation_size: UINT64,
    file_context: *mut PVOID,
    file_info: *mut FSP_FSCTL_FILE_INFO,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null()
            || file_name.is_null()
            || file_context.is_null()
            || file_info.is_null()
        {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        let file_name = unsafe { U16CStr::from_ptr_str(file_name) };
        trace_callback(format!(
            "create path={} create_options={} granted_access={}",
            WinfspFilesystem::normalized_path(file_name),
            create_options,
            granted_access
        ));
        match fs.create(
            file_name,
            create_options,
            granted_access,
            file_attributes,
            allocation_size,
        ) {
            Ok((context, info)) => {
                let context_ptr = (&context as *const WinfspFileContext) as usize;
                unsafe {
                    write_file_context(context, file_context);
                    (*file_info.cast::<FSP_FSCTL_OPEN_FILE_INFO>()).FileInfo = info;
                }
                trace_callback(format!(
                    "create status=success context_ptr=0x{context_ptr:x}"
                ));
                STATUS_SUCCESS
            }
            Err(status) => {
                trace_callback(format!("create status=0x{:08X}", status as u32));
                status
            }
        }
    })
}

unsafe extern "C" fn winfsp_open(
    file_system: *mut FSP_FILE_SYSTEM,
    file_name: PWSTR,
    create_options: UINT32,
    granted_access: UINT32,
    file_context: *mut PVOID,
    file_info: *mut FSP_FSCTL_FILE_INFO,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null()
            || file_name.is_null()
            || file_context.is_null()
            || file_info.is_null()
        {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        let file_name = unsafe { U16CStr::from_ptr_str(file_name) };
        trace_callback(format!(
            "open path={} create_options={} granted_access={}",
            WinfspFilesystem::normalized_path(file_name),
            create_options,
            granted_access
        ));
        match fs.open(file_name, create_options, granted_access) {
            Ok((context, info)) => {
                let context_ptr = (&context as *const WinfspFileContext) as usize;
                unsafe {
                    write_file_context(context, file_context);
                    (*file_info.cast::<FSP_FSCTL_OPEN_FILE_INFO>()).FileInfo = info;
                }
                trace_callback(format!("open status=success context_ptr=0x{context_ptr:x}"));
                STATUS_SUCCESS
            }
            Err(status) => {
                trace_callback(format!("open status=0x{:08X}", status as u32));
                status
            }
        }
    })
}

unsafe extern "C" fn winfsp_overwrite(
    _file_system: *mut FSP_FILE_SYSTEM,
    _file_context: PVOID,
    _file_attributes: UINT32,
    _replace_file_attributes: u8,
    _allocation_size: UINT64,
    _ea: PFILE_FULL_EA_INFORMATION,
    _ea_length: ULONG,
    _file_info: *mut FSP_FSCTL_FILE_INFO,
) -> NTSTATUS {
    trace_callback("overwrite status=access_denied");
    STATUS_ACCESS_DENIED
}

unsafe extern "C" fn winfsp_cleanup(
    _file_system: *mut FSP_FILE_SYSTEM,
    _file_context: PVOID,
    _file_name: PWSTR,
    _flags: ULONG,
) {
}

unsafe extern "C" fn winfsp_close(file_system: *mut FSP_FILE_SYSTEM, file_context: PVOID) {
    let _ = std::panic::catch_unwind(|| {
        if file_system.is_null() || file_context.is_null() {
            return;
        }
        trace_callback(format!("close context_ptr=0x{:x}", file_context as usize));
        let fs = unsafe { fs_context(file_system) };
        let file_context = unsafe { take_file_context(file_context) };
        fs.close(&file_context);
    });
}

unsafe extern "C" fn winfsp_read(
    file_system: *mut FSP_FILE_SYSTEM,
    file_context: PVOID,
    buffer: PVOID,
    offset: UINT64,
    length: ULONG,
    bytes_transferred: PULONG,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null() || file_context.is_null() || buffer.is_null() {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        trace_callback(format!(
            "read context_ptr=0x{:x} offset={} length={}",
            file_context as usize, offset, length
        ));
        let file_context = unsafe { borrow_file_context(file_context) };
        let buffer =
            unsafe { std::slice::from_raw_parts_mut(buffer.cast::<u8>(), length as usize) };
        if !bytes_transferred.is_null() {
            unsafe {
                bytes_transferred.write(0);
            }
        }
        match fs.read(file_context, buffer, offset) {
            Ok(read) => {
                if !bytes_transferred.is_null() {
                    unsafe {
                        bytes_transferred.write(read as ULONG);
                    }
                }
                STATUS_SUCCESS
            }
            Err(status) => status,
        }
    })
}

unsafe extern "C" fn winfsp_write(
    _file_system: *mut FSP_FILE_SYSTEM,
    _file_context: PVOID,
    _buffer: PVOID,
    _offset: UINT64,
    _length: ULONG,
    _write_to_end_of_file: u8,
    _constrained_io: u8,
    bytes_transferred: PULONG,
    _file_info: *mut FSP_FSCTL_FILE_INFO,
) -> NTSTATUS {
    if !bytes_transferred.is_null() {
        unsafe {
            bytes_transferred.write(0);
        }
    }
    trace_callback("write status=access_denied");
    STATUS_ACCESS_DENIED
}

unsafe extern "C" fn winfsp_flush(
    file_system: *mut FSP_FILE_SYSTEM,
    file_context: PVOID,
    file_info: *mut FSP_FSCTL_FILE_INFO,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null() || file_info.is_null() {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        let context = if file_context.is_null() {
            None
        } else {
            Some(unsafe { borrow_file_context(file_context) })
        };
        match fs.flush(context) {
            Ok(info) => {
                unsafe {
                    file_info.write(info);
                }
                STATUS_SUCCESS
            }
            Err(status) => status,
        }
    })
}

unsafe extern "C" fn winfsp_get_file_info(
    file_system: *mut FSP_FILE_SYSTEM,
    file_context: PVOID,
    file_info: *mut FSP_FSCTL_FILE_INFO,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null() || file_context.is_null() || file_info.is_null() {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        let file_context = unsafe { borrow_file_context(file_context) };
        match fs.get_file_info(file_context) {
            Ok(info) => {
                unsafe {
                    file_info.write(info);
                }
                STATUS_SUCCESS
            }
            Err(status) => status,
        }
    })
}

unsafe extern "C" fn winfsp_get_security(
    file_system: *mut FSP_FILE_SYSTEM,
    file_context: PVOID,
    security_descriptor: PSECURITY_DESCRIPTOR,
    security_descriptor_size: *mut SIZE_T,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null() || file_context.is_null() {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        let _file_context = unsafe { borrow_file_context(file_context) };
        match copy_security_descriptor(
            fs.security_descriptor.as_ref(),
            security_descriptor,
            security_descriptor_size,
        ) {
            Ok(()) => STATUS_SUCCESS,
            Err(status) => status,
        }
    })
}

unsafe extern "C" fn winfsp_set_basic_info(
    _file_system: *mut FSP_FILE_SYSTEM,
    _file_context: PVOID,
    _file_attributes: UINT32,
    _creation_time: UINT64,
    _last_access_time: UINT64,
    _last_write_time: UINT64,
    _change_time: UINT64,
    _file_info: *mut FSP_FSCTL_FILE_INFO,
) -> NTSTATUS {
    trace_callback("set_basic_info status=access_denied");
    STATUS_ACCESS_DENIED
}

unsafe extern "C" fn winfsp_set_file_size(
    _file_system: *mut FSP_FILE_SYSTEM,
    _file_context: PVOID,
    _new_size: UINT64,
    _set_allocation_size: u8,
    _file_info: *mut FSP_FSCTL_FILE_INFO,
) -> NTSTATUS {
    trace_callback("set_file_size status=access_denied");
    STATUS_ACCESS_DENIED
}

unsafe extern "C" fn winfsp_can_delete(
    _file_system: *mut FSP_FILE_SYSTEM,
    _file_context: PVOID,
    _file_name: PWSTR,
) -> NTSTATUS {
    trace_callback("can_delete status=access_denied");
    STATUS_ACCESS_DENIED
}

unsafe extern "C" fn winfsp_rename(
    _file_system: *mut FSP_FILE_SYSTEM,
    _file_context: PVOID,
    _file_name: PWSTR,
    _new_file_name: PWSTR,
    _replace_if_exists: u8,
) -> NTSTATUS {
    trace_callback("rename status=access_denied");
    STATUS_ACCESS_DENIED
}

unsafe extern "C" fn winfsp_set_security(
    _file_system: *mut FSP_FILE_SYSTEM,
    _file_context: PVOID,
    _security_information: u32,
    _modification_descriptor: PSECURITY_DESCRIPTOR,
) -> NTSTATUS {
    trace_callback("set_security status=access_denied");
    STATUS_ACCESS_DENIED
}

unsafe extern "C" fn winfsp_read_directory(
    file_system: *mut FSP_FILE_SYSTEM,
    file_context: PVOID,
    _pattern: PWSTR,
    marker: PWSTR,
    buffer: PVOID,
    length: ULONG,
    bytes_transferred: PULONG,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null() || file_context.is_null() || buffer.is_null() {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        let file_context = unsafe { borrow_file_context(file_context) };
        let marker = if marker.is_null() {
            None
        } else {
            Some(unsafe { U16CStr::from_ptr_str(marker) })
        };
        trace_callback(format!(
            "read_directory path={} marker={}",
            file_context.path,
            marker
                .map(|value| value.to_string_lossy())
                .unwrap_or_else(|| "<none>".to_owned())
        ));
        match fs.read_directory(file_context, marker, buffer, length, bytes_transferred) {
            Ok(()) => STATUS_SUCCESS,
            Err(status) => status,
        }
    })
}

unsafe extern "C" fn winfsp_get_dir_info_by_name(
    file_system: *mut FSP_FILE_SYSTEM,
    file_context: PVOID,
    file_name: PWSTR,
    dir_info: *mut FSP_FSCTL_DIR_INFO,
) -> NTSTATUS {
    catch_panic_status!({
        if file_system.is_null()
            || file_context.is_null()
            || file_name.is_null()
            || dir_info.is_null()
        {
            return STATUS_INVALID_PARAMETER;
        }
        let fs = unsafe { fs_context(file_system) };
        let file_context = unsafe { borrow_file_context(file_context) };
        let file_name = unsafe { U16CStr::from_ptr_str(file_name) };
        trace_callback(format!(
            "get_dir_info_by_name parent={} child={}",
            file_context.path,
            file_name.to_string_lossy()
        ));
        match fs.get_dir_info_by_name(file_context, file_name, dir_info) {
            Ok(()) => STATUS_SUCCESS,
            Err(status) => status,
        }
    })
}

fn build_winfsp_interface() -> FSP_FILE_SYSTEM_INTERFACE {
    FSP_FILE_SYSTEM_INTERFACE {
        GetVolumeInfo: Some(winfsp_get_volume_info),
        SetVolumeLabelW: Some(winfsp_set_volume_label),
        GetSecurityByName: Some(winfsp_get_security_by_name),
        Create: Some(winfsp_create),
        Open: Some(winfsp_open),
        OverwriteEx: Some(winfsp_overwrite),
        Cleanup: Some(winfsp_cleanup),
        Close: Some(winfsp_close),
        Read: Some(winfsp_read),
        Write: Some(winfsp_write),
        Flush: Some(winfsp_flush),
        GetFileInfo: Some(winfsp_get_file_info),
        SetBasicInfo: Some(winfsp_set_basic_info),
        SetFileSize: Some(winfsp_set_file_size),
        CanDelete: Some(winfsp_can_delete),
        Rename: Some(winfsp_rename),
        GetSecurity: Some(winfsp_get_security),
        SetSecurity: Some(winfsp_set_security),
        ReadDirectory: Some(winfsp_read_directory),
        GetDirInfoByName: Some(winfsp_get_dir_info_by_name),
        CreateEx: None,
        ..Default::default()
    }
}

impl MountRuntime {
    pub async fn mount_winfsp_filesystem<P: AsRef<Path>>(
        self: Arc<Self>,
        mount_path: P,
        service_name: &str,
        _allow_other: bool,
    ) -> Result<WindowsWinfspMountedFilesystem> {
        let mount_path = mount_path.as_ref().to_path_buf();
        info!(
            mount_path = %mount_path.display(),
            service_name,
            "initializing WinFSP mount"
        );
        trace_callback(format!(
            "mount_start path={} service={service_name}",
            mount_path.display()
        ));
        init().map_err(|error| {
            Error::new(
                ErrorKind::NotFound,
                format!("failed to initialize WinFSP runtime: {error}"),
            )
        })?;

        let security_descriptor = Arc::new(
            SecurityDescriptor::from_wstr(
                U16CString::from_str(DEFAULT_SECURITY_DESCRIPTOR)
                    .map_err(|_| {
                        Error::new(
                            ErrorKind::InvalidInput,
                            "invalid default WinFSP security descriptor",
                        )
                    })?
                    .as_ucstr(),
            )
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("failed to construct WinFSP security descriptor: {error}"),
                )
            })?,
        );

        let volume_label = Arc::new(
            U16CString::from_str(service_name)
                .unwrap_or_else(|_| U16CString::from_str(FILE_SYSTEM_NAME).expect("static label")),
        );
        let runtime_handle = Handle::current();
        let context = WinfspFilesystem::new(
            Arc::clone(&self),
            runtime_handle,
            security_descriptor,
            Arc::clone(&volume_label),
        );
        match self.getattr("/") {
            Ok(root_attributes) => {
                let root_entries = self
                    .readdir("/")
                    .unwrap_or_default()
                    .into_iter()
                    .map(|entry| entry.name)
                    .collect::<Vec<_>>();
                info!(
                    mount_path = %mount_path.display(),
                    root_inode = root_attributes.inode,
                    root_kind = ?root_attributes.kind,
                    root_entries = ?root_entries,
                    "WinFSP root self-check before mount"
                );
            }
            Err(error) => {
                warn!(
                    mount_path = %mount_path.display(),
                    ?error,
                    "WinFSP root self-check failed before mount"
                );
            }
        }
        let mountpoint_spec = legacy_winfsp_mountpoint_spec(&mount_path);
        info!(
            mount_path = %mount_path.display(),
            winfsp_mountpoint = mountpoint_spec,
            service_name,
            mount_exists = mount_path.exists(),
            mount_is_dir = mount_path.is_dir(),
            "starting WinFSP filesystem"
        );
        trace_callback(format!(
            "set_mount_point path={} mountpoint={mountpoint_spec}",
            mount_path.display()
        ));
        let mountpoint = U16CString::from_str(&mountpoint_spec).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "mountpoint {} resolved to an invalid WinFSP spec",
                    mount_path.display()
                ),
            )
        })?;
        let allow_open_in_kernel_mode = winfsp_allow_open_in_kernel_mode();
        let minimal_volume_params = winfsp_use_minimal_volume_params();
        let mut params = WinfspParams::default();
        configure_wrapper_volume_params(&mut params, service_name, allow_open_in_kernel_mode)?;
        let wrapper_mountpoint_spec = winfsp_mountpoint_spec(&mount_path);
        let read_only_volume = winfsp_read_only_volume();
        info!(?params, "WinFSP wrapper raw params");
        info!(
            mount_path = %mount_path.display(),
            service_name,
            mountpoint_spec_legacy = mountpoint_spec,
            mountpoint_spec_wrapper = wrapper_mountpoint_spec,
            security_descriptor_len = context.security_descriptor.len(),
            create_defined = <WinfspFilesystem as FileSystemInterface>::CREATE_DEFINED,
            create_ex_defined = <WinfspFilesystem as FileSystemInterface>::CREATE_EX_DEFINED,
            open_defined = <WinfspFilesystem as FileSystemInterface>::OPEN_DEFINED,
            get_security_by_name_defined = <WinfspFilesystem as FileSystemInterface>::GET_SECURITY_BY_NAME_DEFINED,
            read_directory_defined = <WinfspFilesystem as FileSystemInterface>::READ_DIRECTORY_DEFINED,
            read_only_volume,
            persistent_acls = true,
            always_use_double_buffering = true,
            allow_open_in_kernel_mode,
            minimal_volume_params,
            volume_params_version = winfsp_volume_params_version(),
            volume_params_struct_size = std::mem::size_of::<FSP_FSCTL_VOLUME_PARAMS>() as u16,
            sector_size = 4096u16,
            sectors_per_allocation_unit = 1u16,
            file_info_timeout_ms = 1000u32,
            dir_info_timeout_ms = 1000u32,
            volume_info_timeout_ms = 1000u32,
            security_timeout_ms = 1000u32,
            "WinFSP startup parameters"
        );

        let start_wrapper_backend = || -> Result<WinfspBackendHandle> {
            let file_system =
                FileSystem::start(params, Some(mountpoint.as_ucstr()), context.clone()).map_err(
                    |status| {
                        Error::other(format!(
                            "failed to start WinFSP wrapper mount at {}: 0x{:08X}",
                            mount_path.display(),
                            status as u32
                        ))
                    },
                )?;
            trace_callback(format!(
                "dispatcher_mountpoint path={} mountpoint={mountpoint_spec}",
                mount_path.display()
            ));
            Ok(WinfspBackendHandle::Wrapper(Box::new(file_system)))
        };

        let start_raw_backend = || -> Result<WinfspBackendHandle> {
            let mut volume_params = FSP_FSCTL_VOLUME_PARAMS::default();
            configure_volume_params(&mut volume_params, service_name, allow_open_in_kernel_mode);

            let interface = Box::into_raw(Box::new(build_winfsp_interface()));
            let context_ptr = Box::into_raw(Box::new(context.clone()));
            let mut file_system = null_mut();
            let device_name = U16CString::from_str(WINFSP_DISK_DEVICE_NAME)
                .expect("static WinFSP disk device name");

            let create_status = unsafe {
                FspFileSystemCreate(
                    device_name.as_ptr().cast_mut(),
                    &volume_params,
                    interface,
                    &mut file_system,
                )
            };
            if create_status != STATUS_SUCCESS {
                unsafe {
                    drop(Box::from_raw(context_ptr));
                    drop(Box::from_raw(interface));
                }
                return Err(Error::other(format!(
                    "failed to create raw WinFSP filesystem at {}: 0x{:08X}",
                    mount_path.display(),
                    create_status as u32
                )));
            }

            unsafe {
                (*file_system).UserContext = context_ptr.cast();
                FspDebugLogSetHandle(GetStdHandle(STD_ERROR_HANDLE));
                FspFileSystemSetDebugLogF(file_system, u32::MAX);
                FspFileSystemSetOperationGuardStrategyF(
                    file_system,
                    FSP_FILE_SYSTEM_OPERATION_GUARD_STRATEGY_FSP_FILE_SYSTEM_OPERATION_GUARD_STRATEGY_FINE,
                );
            }

            let mount_status =
                unsafe { FspFileSystemSetMountPoint(file_system, mountpoint.as_ptr().cast_mut()) };
            if mount_status != STATUS_SUCCESS {
                unsafe {
                    FspFileSystemDelete(file_system);
                    drop(Box::from_raw(context_ptr));
                    drop(Box::from_raw(interface));
                }
                return Err(Error::other(format!(
                    "failed to set raw WinFSP mountpoint at {}: 0x{:08X}",
                    mount_path.display(),
                    mount_status as u32
                )));
            }

            let dispatch_status = unsafe { FspFileSystemStartDispatcher(file_system, 0) };
            if dispatch_status != STATUS_SUCCESS {
                unsafe {
                    FspFileSystemRemoveMountPoint(file_system);
                    FspFileSystemDelete(file_system);
                    drop(Box::from_raw(context_ptr));
                    drop(Box::from_raw(interface));
                }
                return Err(Error::other(format!(
                    "failed to start raw WinFSP dispatcher at {}: 0x{:08X}",
                    mount_path.display(),
                    dispatch_status as u32
                )));
            }

            trace_callback(format!(
                "dispatcher_mountpoint path={} mountpoint={mountpoint_spec}",
                mount_path.display()
            ));
            Ok(WinfspBackendHandle::Raw {
                file_system,
                context: context_ptr,
                interface,
            })
        };

        let force_wrapper_backend =
            std::env::var("FILMUVFS_WINFSP_USE_WRAPPER").as_deref() == Ok("1");
        let allow_wrapper_fallback =
            std::env::var("FILMUVFS_WINFSP_ALLOW_WRAPPER_FALLBACK").as_deref() == Ok("1");

        let backend = if force_wrapper_backend {
            info!("FILMUVFS_WINFSP_USE_WRAPPER=1; forcing winfsp_wrs host backend");
            start_wrapper_backend()
        } else {
            match start_raw_backend() {
                Ok(raw_backend) => {
                    info!("using raw WinFSP host backend");
                    Ok(raw_backend)
                }
                Err(raw_error) => {
                    if allow_wrapper_fallback {
                        warn!(
                            ?raw_error,
                            "raw WinFSP host backend failed; FILMUVFS_WINFSP_ALLOW_WRAPPER_FALLBACK=1 so falling back to winfsp_wrs"
                        );
                        start_wrapper_backend()
                    } else {
                        Err(Error::other(format!(
                            "raw WinFSP host backend failed and wrapper fallback is disabled (set FILMUVFS_WINFSP_ALLOW_WRAPPER_FALLBACK=1 to re-enable fallback): {raw_error}"
                        )))
                    }
                }
            }
        };

        let backend = match backend {
            Ok(backend) => backend,
            Err(error) => {
                warn!(
                    mount_path = %mount_path.display(),
                    service_name,
                    mount_exists = mount_path.exists(),
                    mount_is_dir = mount_path.is_dir(),
                    ?error,
                    "WinFSP filesystem startup failed"
                );
                return Err(error);
            }
        };

        info!(
            mount_path = %mount_path.display(),
            winfsp_mountpoint = mountpoint_spec,
            service_name,
            "WinFSP filesystem started"
        );
        trace_callback(format!(
            "dispatcher_started path={} mountpoint={mountpoint_spec}",
            mount_path.display()
        ));

        match std::fs::read_dir(&mount_path) {
            Ok(read_dir) => {
                let root_preview = read_dir
                    .filter_map(|entry| entry.ok())
                    .filter_map(|entry| entry.file_name().into_string().ok())
                    .take(8)
                    .collect::<Vec<_>>();
                info!(
                    mount_path = %mount_path.display(),
                    root_preview = ?root_preview,
                    "WinFSP post-start directory probe succeeded"
                );
            }
            Err(error) => {
                warn!(
                    mount_path = %mount_path.display(),
                    ?error,
                    "WinFSP post-start directory probe failed"
                );
            }
        }

        Ok(WindowsWinfspMountedFilesystem {
            mount_path,
            backend: Some(backend),
        })
    }
}

fn configure_volume_params(
    volume_params: &mut FSP_FSCTL_VOLUME_PARAMS,
    service_name: &str,
    allow_open_in_kernel_mode: bool,
) {
    let minimal_params = winfsp_use_minimal_volume_params();
    let read_only_volume = winfsp_read_only_volume();
    volume_params.Version = winfsp_volume_params_version();
    volume_params.set_CaseSensitiveSearch(0);
    volume_params.set_CasePreservedNames(1);
    volume_params.set_UnicodeOnDisk(1);
    volume_params.set_PersistentAcls(u32::from(!minimal_params));
    volume_params.set_PostCleanupWhenModifiedOnly(u32::from(!minimal_params));
    volume_params.set_AlwaysUseDoubleBuffering(u32::from(!minimal_params));
    volume_params.set_AllowOpenInKernelMode(u32::from(allow_open_in_kernel_mode));
    volume_params.set_ReadOnlyVolume(u32::from(read_only_volume));
    volume_params.set_UmFileContextIsFullContext(0);
    // Match the working WinFSP wrapper defaults: callbacks receive the user context
    // through UserContext2 rather than the legacy file-context slot.
    volume_params.set_UmFileContextIsUserContext2(1);
    volume_params.SectorSize = 4096;
    volume_params.SectorsPerAllocationUnit = 1;
    volume_params.MaxComponentLength = 255;
    volume_params.VolumeCreationTime = filetime_now();
    volume_params.VolumeSerialNumber = stable_volume_serial(service_name);
    if minimal_params {
        volume_params.FileInfoTimeout = 0;
        volume_params.DirInfoTimeout = 0;
        volume_params.VolumeInfoTimeout = 0;
        volume_params.SecurityTimeout = 0;
    } else {
        volume_params.FileInfoTimeout = 1000;
        volume_params.DirInfoTimeout = 1000;
        volume_params.VolumeInfoTimeout = 1000;
        volume_params.SecurityTimeout = 1000;
    }

    let file_system_name = U16CString::from_str(FILE_SYSTEM_NAME).expect("static FS name");
    let name_slice = file_system_name.as_slice();
    let copy_len = name_slice.len().min(volume_params.FileSystemName.len());
    volume_params.FileSystemName[..copy_len].copy_from_slice(&name_slice[..copy_len]);
}

fn configure_wrapper_volume_params(
    params: &mut WinfspParams,
    service_name: &str,
    allow_open_in_kernel_mode: bool,
) -> Result<()> {
    let minimal_params = winfsp_use_minimal_volume_params();
    let read_only_volume = winfsp_read_only_volume();
    let volume_params = &mut params.volume_params;
    let mut builder = volume_params
        .set_version(winfsp_volume_params_version())
        .set_case_sensitive_search(false)
        .set_case_preserved_names(true)
        .set_unicode_on_disk(true)
        .set_persistent_acls(!minimal_params)
        .set_post_cleanup_when_modified_only(!minimal_params)
        .set_always_use_double_buffering(!minimal_params)
        .set_allow_open_in_kernel_mode(allow_open_in_kernel_mode)
        .set_read_only_volume(read_only_volume)
        .set_sector_size(4096)
        .set_sectors_per_allocation_unit(1)
        .set_max_component_length(255)
        .set_volume_creation_time(filetime_now())
        .set_volume_serial_number(stable_volume_serial(service_name));

    if minimal_params {
        builder = builder
            .set_file_info_timeout(0)
            .set_dir_info_timeout(0)
            .set_volume_info_timeout(0)
            .set_security_timeout(0);
    } else {
        builder = builder
            .set_file_info_timeout(1000)
            .set_dir_info_timeout(1000)
            .set_volume_info_timeout(1000)
            .set_security_timeout(1000);
    }
    let _ = builder;

    let file_system_name = U16CString::from_str(FILE_SYSTEM_NAME).expect("static FS name");
    volume_params
        .set_file_system_name(file_system_name.as_ucstr())
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "invalid WinFSP file system name"))?;

    Ok(())
}

fn winfsp_mountpoint_spec(mount_path: &Path) -> String {
    let normalized = mount_path
        .display()
        .to_string()
        .trim_end_matches('\\')
        .to_owned();
    if normalized.len() == 2
        && normalized.as_bytes()[1] == b':'
        && normalized.as_bytes()[0].is_ascii_alphabetic()
    {
        normalized
    } else {
        format!(r"\\.\{normalized}")
    }
}

fn legacy_winfsp_mountpoint_spec(mount_path: &Path) -> String {
    mount_path
        .display()
        .to_string()
        .trim_end_matches('\\')
        .to_owned()
}

fn winfsp_allow_open_in_kernel_mode() -> bool {
    let Some(raw) = std::env::var_os("FILMUVFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE") else {
        return true;
    };
    let parsed = raw.to_string_lossy().trim().to_ascii_lowercase();
    matches!(parsed.as_str(), "1" | "true" | "yes" | "on")
}

fn winfsp_use_minimal_volume_params() -> bool {
    let Some(raw) = std::env::var_os("FILMUVFS_WINFSP_MINIMAL_PARAMS") else {
        return false;
    };
    let parsed = raw.to_string_lossy().trim().to_ascii_lowercase();
    matches!(parsed.as_str(), "1" | "true" | "yes" | "on")
}

fn winfsp_read_only_volume() -> bool {
    let Some(raw) = std::env::var_os("FILMUVFS_WINFSP_READ_ONLY") else {
        return true;
    };
    let parsed = raw.to_string_lossy().trim().to_ascii_lowercase();
    !matches!(parsed.as_str(), "0" | "false" | "no" | "off")
}

fn winfsp_volume_params_version() -> u16 {
    std::mem::size_of::<FSP_FSCTL_VOLUME_PARAMS>() as u16
}

fn stable_volume_serial(seed: &str) -> u32 {
    let mut hash = 0x811C9DC5u32;
    for byte in seed.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

fn raw_file_info_from_attributes(
    attributes: &crate::mount::MountAttributes,
) -> FSP_FSCTL_FILE_INFO {
    let now = filetime_now();
    let size = match attributes.kind {
        MountNodeKind::Directory => 0,
        MountNodeKind::File => attributes.size_bytes,
    };
    FSP_FSCTL_FILE_INFO {
        FileAttributes: file_attributes_from_kind(attributes.kind).0,
        ReparseTag: 0,
        AllocationSize: size,
        FileSize: size,
        CreationTime: now,
        LastAccessTime: now,
        LastWriteTime: now,
        ChangeTime: now,
        IndexNumber: attributes.inode,
        HardLinks: if attributes.kind == MountNodeKind::Directory {
            2
        } else {
            1
        },
        EaSize: 0,
    }
}

fn raw_file_info_for_volume() -> FSP_FSCTL_FILE_INFO {
    let now = filetime_now();
    FSP_FSCTL_FILE_INFO {
        FileAttributes: FileAttributes::DIRECTORY.0,
        ReparseTag: 0,
        AllocationSize: 0,
        FileSize: 0,
        CreationTime: now,
        LastAccessTime: now,
        LastWriteTime: now,
        ChangeTime: now,
        IndexNumber: 0,
        HardLinks: 0,
        EaSize: 0,
    }
}

fn dir_info_from_entry(file_info: FSP_FSCTL_FILE_INFO, file_name: &str) -> DirInfo {
    let mut info = DirInfo {
        size: 0,
        file_info,
        _padding: [0; 24],
        file_name: [0; 255],
    };

    let mut i = 0;
    for c in file_name.encode_utf16() {
        if i >= info.file_name.len() {
            break;
        }
        info.file_name[i] = c;
        i += 1;
    }
    info.size = (std::mem::size_of::<FSP_FSCTL_DIR_INFO>() + i * std::mem::size_of::<u16>()) as u16;
    info
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct DirInfo {
    size: u16,
    file_info: FSP_FSCTL_FILE_INFO,
    _padding: [u8; 24],
    file_name: [u16; 255],
}

fn file_attributes_from_kind(kind: MountNodeKind) -> FileAttributes {
    match kind {
        MountNodeKind::Directory => FileAttributes::DIRECTORY,
        MountNodeKind::File => FileAttributes::READONLY | FileAttributes::ARCHIVE,
    }
}

fn ntstatus_from_mount_error(error: MountRuntimeError) -> NTSTATUS {
    match error {
        MountRuntimeError::PathNotFound { .. } | MountRuntimeError::InodeNotFound { .. } => {
            STATUS_OBJECT_NAME_NOT_FOUND
        }
        MountRuntimeError::NotDirectory { .. } => STATUS_NOT_A_DIRECTORY,
        MountRuntimeError::NotFile { .. } => STATUS_FILE_IS_A_DIRECTORY,
        MountRuntimeError::MissingDetails { .. }
        | MountRuntimeError::MissingUrl { .. }
        | MountRuntimeError::Io { .. } => STATUS_IO_DEVICE_ERROR,
        MountRuntimeError::ReadAborted { .. } => STATUS_CANCELLED,
        MountRuntimeError::HandleNotFound { .. }
        | MountRuntimeError::HandleInodeMismatch { .. }
        | MountRuntimeError::InvalidName { .. } => STATUS_INVALID_PARAMETER,
        MountRuntimeError::StaleLease { .. } => STATUS_DEVICE_NOT_READY,
        MountRuntimeError::ShuttingDown => STATUS_DEVICE_NOT_READY,
    }
}

fn strip_stream_suffix(path: &str) -> &str {
    let file_name = path.rsplit('/').next().unwrap_or(path);
    let Some(stream_index) = file_name.find(':') else {
        return path;
    };
    let suffix_offset = file_name.len().saturating_sub(stream_index);
    &path[..path.len().saturating_sub(suffix_offset)]
}

#[must_use]
pub const fn winfsp_backend_compiled() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{
        legacy_winfsp_mountpoint_spec, ntstatus_from_mount_error, stable_volume_serial,
        strip_stream_suffix, winfsp_mountpoint_spec, WinfspFilesystem, STATUS_CANCELLED,
    };
    use std::{path::PathBuf, sync::Arc};

    use tokio::runtime::Runtime;
    use winfsp_wrs::{SecurityDescriptor, U16CString};

    use crate::{catalog::state::CatalogStateStore, mount::MountRuntime};

    #[test]
    fn strips_named_stream_suffixes() {
        assert_eq!(
            strip_stream_suffix("movies/Avatar.mkv::$DATA"),
            "movies/Avatar.mkv"
        );
        assert_eq!(
            strip_stream_suffix("movies/Avatar.mkv"),
            "movies/Avatar.mkv"
        );
    }

    #[test]
    fn normalizes_root_and_relative_paths() {
        let runtime = Runtime::new().expect("runtime should build");
        let fs = WinfspFilesystem::new(
            Arc::new(MountRuntime::new(
                Arc::new(CatalogStateStore::new()),
                "session".to_owned(),
            )),
            runtime.handle().clone(),
            Arc::new(
                SecurityDescriptor::from_wstr(
                    U16CString::from_str(super::DEFAULT_SECURITY_DESCRIPTOR)
                        .expect("static descriptor is valid")
                        .as_ucstr(),
                )
                .expect("descriptor should build"),
            ),
            Arc::new(U16CString::from_str(super::FILE_SYSTEM_NAME).expect("static label is valid")),
        );

        let root = U16CString::from_str("\\").expect("root path is valid");
        assert_eq!(WinfspFilesystem::normalized_path(root.as_ucstr()), "/");

        let file = U16CString::from_str("\\movies\\Avatar.mkv::$DATA").expect("path is valid");
        assert_eq!(
            WinfspFilesystem::normalized_path(file.as_ucstr()),
            "/movies/Avatar.mkv"
        );

        let relative = U16CString::from_str("shows\\Show\\Episode.mkv").expect("path is valid");
        assert_eq!(
            WinfspFilesystem::normalized_path(relative.as_ucstr()),
            "/shows/Show/Episode.mkv"
        );

        let _ = PathBuf::from("unused");
        let _ = fs;
    }

    #[test]
    fn derives_stable_volume_serials() {
        assert_eq!(
            stable_volume_serial("filmuvfs"),
            stable_volume_serial("filmuvfs")
        );
        assert_ne!(
            stable_volume_serial("filmuvfs"),
            stable_volume_serial("other")
        );
    }

    #[test]
    fn converts_directory_mountpoints_to_winfsp_specs() {
        assert_eq!(
            winfsp_mountpoint_spec(&PathBuf::from(r"E:\FilmuCoreVFS")),
            r"\\.\E:\FilmuCoreVFS"
        );
        assert_eq!(
            legacy_winfsp_mountpoint_spec(&PathBuf::from(r"E:\FilmuCoreVFS")),
            r"E:\FilmuCoreVFS"
        );
        assert_eq!(winfsp_mountpoint_spec(&PathBuf::from(r"X:")), r"X:");
        assert_eq!(winfsp_mountpoint_spec(&PathBuf::from(r"X:\")), r"X:");
    }

    #[test]
    fn maps_read_aborted_to_cancelled_status() {
        assert_eq!(
            ntstatus_from_mount_error(crate::mount::MountRuntimeError::ReadAborted {
                path: "/movies/Avatar.mkv".to_owned(),
            }),
            STATUS_CANCELLED
        );
    }
}
