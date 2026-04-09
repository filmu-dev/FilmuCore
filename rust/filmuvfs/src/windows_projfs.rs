use std::{
    ffi::{c_void, OsStr},
    mem::size_of,
    os::windows::ffi::OsStrExt,
    path::{Path, PathBuf},
    ptr::{null, null_mut},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use dashmap::{mapref::entry::Entry, DashMap};
use tokio::runtime::Handle;
use tracing::{debug, info, warn};
use windows_sys::{
    core::{GUID, HRESULT, PCWSTR},
    Win32::{
        Foundation::{
            ERROR_DIRECTORY, ERROR_FILE_NOT_FOUND, ERROR_INSUFFICIENT_BUFFER, ERROR_INVALID_HANDLE,
            ERROR_INVALID_PARAMETER, ERROR_IO_PENDING, ERROR_NOT_ENOUGH_MEMORY,
            ERROR_NOT_SUPPORTED, ERROR_PATH_NOT_FOUND, S_OK,
        },
        Storage::{
            FileSystem::{FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_READONLY},
            ProjectedFileSystem::{
                PrjAllocateAlignedBuffer, PrjCompleteCommand, PrjFileNameMatch,
                PrjFillDirEntryBuffer, PrjFreeAlignedBuffer, PrjMarkDirectoryAsPlaceholder,
                PrjStartVirtualizing, PrjStopVirtualizing, PrjWriteFileData,
                PrjWritePlaceholderInfo, PRJ_CALLBACKS, PRJ_CALLBACK_DATA,
                PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN, PRJ_DIR_ENTRY_BUFFER_HANDLE,
                PRJ_FILE_BASIC_INFO, PRJ_FLAG_NONE, PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
                PRJ_NOTIFICATION, PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED,
                PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED,
                PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_NOTIFICATION_MAPPING,
                PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED,
                PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED,
                PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION, PRJ_PLACEHOLDER_INFO,
                PRJ_PLACEHOLDER_VERSION_INFO, PRJ_STARTVIRTUALIZING_OPTIONS,
            },
        },
    },
};

use crate::{
    mount::{MountHandle, MountNodeKind, MountRuntime, MountRuntimeError},
    telemetry::{
        record_windows_projfs_callback, record_windows_projfs_callback_duration,
        record_windows_projfs_notification, record_windows_projfs_stream_handle_event,
    },
};

const WINDOWS_EPOCH_OFFSET_SECS: u64 = 11_644_473_600;
const WINDOWS_PROJFS_SLOW_CALLBACK_WARN_MS: u128 = 250;
// ProjFS can request very large ranges (for example ~4 GiB sentinel-style reads).
// Keep each write bounded, but large enough to avoid excessive upstream round-trips.
const WINDOWS_PROJFS_CALLBACK_WRITE_CHUNK_BYTES: u32 = 8 * 1024 * 1024;
const WINDOWS_PROJFS_LARGE_REQUEST_THRESHOLD_BYTES: u32 = 512 * 1024 * 1024;
const WINDOWS_PROJFS_LARGE_REQUEST_WRITE_CHUNK_BYTES: u32 = 64 * 1024 * 1024;
const WINDOWS_PROJFS_BOOTSTRAP_CHUNK_BYTES: u32 = 1024 * 1024;
const WINDOWS_PROJFS_SENTINEL_LENGTH_FLOOR: u32 = 0xFFFF_0000;
const WINDOWS_PROJFS_SENTINEL_MAX_SERVICE_BYTES: u64 = 512 * 1024;
const WINDOWS_PROJFS_TAIL_PREFETCH_BYTES: u32 = 4 * 1024 * 1024;
const WINDOWS_PROJFS_LARGE_FILE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const WINDOWS_PROJFS_LARGE_FILE_TAIL_SEED_BYTES: u32 = 10 * 1024 * 1024;
const WINDOWS_PROJFS_WRITE_ALIGNMENT_BYTES: u64 = 4096;

pub struct WindowsProjfsMountedFilesystem {
    mount_path: PathBuf,
    namespace_context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    instance: Arc<WindowsProjfsInstance>,
}

impl std::fmt::Debug for WindowsProjfsMountedFilesystem {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WindowsProjfsMountedFilesystem")
            .field("mount_path", &self.mount_path)
            .field("namespace_context", &self.namespace_context)
            .finish()
    }
}

unsafe impl Send for WindowsProjfsMountedFilesystem {}

impl WindowsProjfsMountedFilesystem {
    pub fn mount_path(&self) -> &Path {
        &self.mount_path
    }

    pub async fn unmount(self) -> std::io::Result<()> {
        unsafe {
            PrjStopVirtualizing(self.namespace_context);
        }
        self.instance.shutdown();
        Ok(())
    }
}

#[derive(Clone)]
struct ProjfsDirectoryEntry {
    name: String,
    basic_info: PRJ_FILE_BASIC_INFO,
}

#[derive(Clone, Default)]
struct DirectoryEnumerationState {
    search_expression: Option<String>,
    entries: Vec<ProjfsDirectoryEntry>,
    next_index: usize,
}

#[derive(Clone)]
struct AsyncGetFileDataCommand {
    namespace_context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    command_id: i32,
    data_stream_id: GUID,
    normalized_path: String,
    byte_offset: u64,
    length: u32,
}

unsafe impl Send for AsyncGetFileDataCommand {}
unsafe impl Sync for AsyncGetFileDataCommand {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct GuidKey([u8; 16]);

impl GuidKey {
    fn from_guid(guid: &GUID) -> Self {
        let mut bytes = [0u8; 16];
        bytes[..4].copy_from_slice(&guid.data1.to_le_bytes());
        bytes[4..6].copy_from_slice(&guid.data2.to_le_bytes());
        bytes[6..8].copy_from_slice(&guid.data3.to_le_bytes());
        bytes[8..].copy_from_slice(&guid.data4);
        Self(bytes)
    }
}

const PROJFS_CALLBACKS: PRJ_CALLBACKS = PRJ_CALLBACKS {
    StartDirectoryEnumerationCallback: Some(start_directory_enumeration_callback),
    EndDirectoryEnumerationCallback: Some(end_directory_enumeration_callback),
    GetDirectoryEnumerationCallback: Some(get_directory_enumeration_callback),
    GetPlaceholderInfoCallback: Some(get_placeholder_info_callback),
    GetFileDataCallback: Some(get_file_data_callback),
    QueryFileNameCallback: Some(query_file_name_callback),
    NotificationCallback: Some(notification_callback),
    CancelCommandCallback: None,
};

struct WindowsProjfsInstance {
    mount_path: PathBuf,
    mount_runtime: Arc<MountRuntime>,
    runtime_handle: Handle,
    service_name: String,
    enumerations: DashMap<GuidKey, DirectoryEnumerationState>,
    stream_handles: DashMap<StreamHandleKey, MountHandle>,
    tail_prefetch_started: DashMap<String, ()>,
    _notification_root: Vec<u16>,
    notification_mappings: Vec<PRJ_NOTIFICATION_MAPPING>,
}

unsafe impl Send for WindowsProjfsInstance {}
unsafe impl Sync for WindowsProjfsInstance {}

impl WindowsProjfsInstance {
    fn stream_handle_key(stream_id: &GUID, normalized_path: &str) -> StreamHandleKey {
        StreamHandleKey {
            stream_id: GuidKey::from_guid(stream_id),
            normalized_path: normalized_path.to_ascii_lowercase(),
        }
    }

    fn new(
        mount_path: PathBuf,
        mount_runtime: Arc<MountRuntime>,
        runtime_handle: Handle,
        service_name: String,
    ) -> Self {
        let notification_root = widestring("");
        Self {
            mount_path,
            mount_runtime,
            runtime_handle,
            service_name,
            enumerations: DashMap::new(),
            stream_handles: DashMap::new(),
            tail_prefetch_started: DashMap::new(),
            notification_mappings: vec![PRJ_NOTIFICATION_MAPPING {
                NotificationBitMask: PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION
                    | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED
                    | PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED,
                NotificationRoot: notification_root.as_ptr(),
            }],
            _notification_root: notification_root,
        }
    }

    fn normalize_callback_path(&self, raw: &str) -> String {
        let normalized = raw.trim().replace('\\', "/");
        let trimmed = strip_stream_suffix(normalized.trim_matches('/'));
        if trimmed.is_empty() {
            return "/".to_owned();
        }

        let mount_root = self
            .mount_path
            .to_string_lossy()
            .replace('\\', "/")
            .trim_matches('/')
            .to_owned();
        let trimmed_lower = trimmed.to_ascii_lowercase();
        let mount_root_lower = mount_root.to_ascii_lowercase();
        if !mount_root_lower.is_empty() {
            if trimmed_lower == mount_root_lower {
                return "/".to_owned();
            }
            let mount_root_prefix = format!("{mount_root_lower}/");
            if trimmed_lower.starts_with(&mount_root_prefix) {
                let relative = &trimmed[mount_root.len() + 1..];
                return format!("/{}", relative.trim_matches('/'));
            }
        }

        format!("/{trimmed}")
    }

    fn start_directory_enumeration(&self, enumeration_id: &GUID, directory_path: &str) -> HRESULT {
        debug!(path = %directory_path, "projfs.start_directory_enumeration");
        self.enumerations.insert(
            GuidKey::from_guid(enumeration_id),
            DirectoryEnumerationState::default(),
        );
        S_OK
    }

    fn end_directory_enumeration(&self, enumeration_id: &GUID) -> HRESULT {
        self.enumerations
            .remove(&GuidKey::from_guid(enumeration_id));
        S_OK
    }

    fn get_directory_enumeration(
        &self,
        directory_path: &str,
        enumeration_id: &GUID,
        search_expression: Option<&str>,
        restart_scan: bool,
        dir_entry_buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
    ) -> HRESULT {
        let started_at = Instant::now();
        let mut state = match self
            .enumerations
            .get_mut(&GuidKey::from_guid(enumeration_id))
        {
            Some(state) => state,
            None => return hresult_from_win32(ERROR_INVALID_PARAMETER),
        };

        let normalized_search = normalize_search_expression(search_expression);
        let should_rebuild_entries = restart_scan
            || state.search_expression != normalized_search
            || (state.next_index == 0 && state.entries.is_empty());
        if should_rebuild_entries {
            match self.build_directory_entries(directory_path, normalized_search.as_deref()) {
                Ok(entries) => {
                    state.entries = entries;
                    state.next_index = 0;
                    state.search_expression = normalized_search;
                    if directory_path == "/" {
                        info!(
                            directory_path,
                            search_expression = search_expression.unwrap_or("*"),
                            restart_scan,
                            entries = state.entries.len(),
                            "projfs.root_directory_enumeration"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        path = %directory_path,
                        search_expression = search_expression.unwrap_or("*"),
                        restart_scan,
                        ?error,
                        "projfs.get_directory_enumeration failed"
                    );
                    let hr = hresult_from_mount_error(error);
                    warn_if_slow_projfs_operation(
                        "GetDirectoryEnumeration",
                        directory_path,
                        started_at.elapsed(),
                        Some(hr),
                    );
                    return hr;
                }
            }
        }

        while state.next_index < state.entries.len() {
            let entry = &state.entries[state.next_index];
            let name = widestring(&entry.name);
            let hr = unsafe {
                PrjFillDirEntryBuffer(name.as_ptr(), &entry.basic_info, dir_entry_buffer)
            };
            if hr == S_OK {
                state.next_index += 1;
                continue;
            }

            if hr == hresult_from_win32(ERROR_INSUFFICIENT_BUFFER) {
                if state.next_index == 0 {
                    return hr;
                }
                break;
            }

            warn!(
                path = %directory_path,
                entry = %entry.name,
                hr = format_hresult(hr),
                "projfs.get_directory_enumeration fill failed"
            );
            warn_if_slow_projfs_operation(
                "GetDirectoryEnumeration",
                directory_path,
                started_at.elapsed(),
                Some(hr),
            );
            return hr;
        }

        warn_if_slow_projfs_operation(
            "GetDirectoryEnumeration",
            directory_path,
            started_at.elapsed(),
            None,
        );
        S_OK
    }

    fn get_placeholder_info(
        &self,
        callback_data: &PRJ_CALLBACK_DATA,
        normalized_path: &str,
    ) -> HRESULT {
        let started_at = Instant::now();
        let attributes = match self.mount_runtime.getattr(normalized_path) {
            Ok(attributes) => attributes,
            Err(error) => {
                warn!(path = %normalized_path, ?error, "projfs.get_placeholder_info failed");
                let hr = hresult_from_mount_error(error);
                warn_if_slow_projfs_operation(
                    "GetPlaceholderInfo",
                    normalized_path,
                    started_at.elapsed(),
                    Some(hr),
                );
                return hr;
            }
        };

        let placeholder = placeholder_info_for_path(
            &self.service_name,
            &attributes.path,
            file_basic_info_from_attributes(&attributes),
        );
        if attributes.kind == MountNodeKind::File {
            warn!(
                path = %normalized_path,
                size_bytes = attributes.size_bytes,
                "projfs.get_placeholder_info file"
            );
        }
        let hr = unsafe {
            PrjWritePlaceholderInfo(
                callback_data.NamespaceVirtualizationContext,
                callback_data.FilePathName,
                &placeholder,
                size_of::<PRJ_PLACEHOLDER_INFO>() as u32,
            )
        };
        if hr != S_OK {
            warn!(
                path = %normalized_path,
                hr = format_hresult(hr),
                "projfs.get_placeholder_info write failed"
            );
        }
        warn_if_slow_projfs_operation(
            "GetPlaceholderInfo",
            normalized_path,
            started_at.elapsed(),
            Some(hr),
        );
        hr
    }

    fn queue_get_file_data(
        self: &Arc<Self>,
        callback_data: &PRJ_CALLBACK_DATA,
        normalized_path: &str,
        byte_offset: u64,
        length: u32,
    ) -> HRESULT {
        let triggering_process_id = callback_data.TriggeringProcessId;
        let triggering_process_image =
            pcwstr_to_string(callback_data.TriggeringProcessImageFileName);
        let callback_flags = callback_data.Flags;
        if byte_offset == 0 {
            warn!(
                path = %normalized_path,
                byte_offset,
                length,
                callback_flags = format_args!("{callback_flags:#x}"),
                triggering_process_id,
                triggering_process_image = %triggering_process_image,
                command_id = callback_data.CommandId,
                "projfs.get_file_data start"
            );
        }

        let request = AsyncGetFileDataCommand {
            namespace_context: callback_data.NamespaceVirtualizationContext,
            command_id: callback_data.CommandId,
            data_stream_id: callback_data.DataStreamId,
            normalized_path: normalized_path.to_owned(),
            byte_offset,
            length,
        };

        let instance = Arc::clone(self);
        self.runtime_handle.spawn(async move {
            let completion_result = instance.get_file_data_for_command(&request).await;
            let complete_hr = unsafe {
                PrjCompleteCommand(
                    request.namespace_context,
                    request.command_id,
                    completion_result,
                    null(),
                )
            };
            if complete_hr != S_OK {
                warn!(
                    path = %request.normalized_path,
                    command_id = request.command_id,
                    completion_result = format_hresult(completion_result),
                    complete_hr = format_hresult(complete_hr),
                    "projfs.get_file_data completion command failed"
                );
            }
        });

        hresult_from_win32(ERROR_IO_PENDING)
    }

    async fn get_file_data_for_command(&self, request: &AsyncGetFileDataCommand) -> HRESULT {
        let started_at = Instant::now();
        let normalized_path = request.normalized_path.as_str();
        let byte_offset = request.byte_offset;
        let length = request.length;

        let handle = match self
            .get_or_create_stream_handle(&request.data_stream_id, normalized_path)
        {
            Ok(handle) => handle,
            Err(error) => {
                let elapsed = started_at.elapsed();
                warn!(path = %normalized_path, byte_offset, length, ?error, "projfs.get_file_data open failed");
                record_windows_projfs_callback("error");
                record_windows_projfs_callback_duration(elapsed, "error");
                warn_if_slow_callback(normalized_path, byte_offset, length, elapsed, "error");
                return hresult_from_mount_error(error);
            }
        };
        if byte_offset == 0 {
            self.schedule_tail_prefetch(normalized_path, &handle);
        }

        let callback_chunk_bytes = if length >= WINDOWS_PROJFS_LARGE_REQUEST_THRESHOLD_BYTES {
            WINDOWS_PROJFS_LARGE_REQUEST_WRITE_CHUNK_BYTES
        } else {
            WINDOWS_PROJFS_CALLBACK_WRITE_CHUNK_BYTES
        };

        if byte_offset == 0 {
            warn!(
                path = %normalized_path,
                byte_offset,
                length,
                handle_id = handle.handle_id,
                inode = handle.inode,
                chunk_bytes = callback_chunk_bytes,
                command_id = request.command_id,
                "projfs.get_file_data read_bytes start"
            );
        }

        let mut next_offset = byte_offset;
        let requested_remaining = u64::from(length);
        let is_sentinel_length = length >= WINDOWS_PROJFS_SENTINEL_LENGTH_FLOOR;
        let mut remaining = match handle.size_bytes {
            Some(file_size) if byte_offset >= file_size => {
                record_windows_projfs_callback("ok");
                record_windows_projfs_callback_duration(started_at.elapsed(), "ok");
                return S_OK;
            }
            Some(file_size) => {
                let available = file_size.saturating_sub(byte_offset);
                if requested_remaining > available {
                    warn!(
                        path = %normalized_path,
                        byte_offset,
                        requested_length = length,
                        clamped_length = available,
                        file_size,
                        "projfs.get_file_data clamping callback request to known file size"
                    );
                }
                requested_remaining.min(available)
            }
            None => requested_remaining,
        };
        let sentinel_tail_seed = if is_sentinel_length && byte_offset == 0 {
            handle.size_bytes.map(|file_size| {
                let configured_tail_seed = if file_size > WINDOWS_PROJFS_LARGE_FILE_BYTES {
                    WINDOWS_PROJFS_LARGE_FILE_TAIL_SEED_BYTES
                } else {
                    WINDOWS_PROJFS_TAIL_PREFETCH_BYTES
                };
                let seed_len = u64::from(configured_tail_seed).min(file_size);
                let seed_offset = file_size.saturating_sub(seed_len);
                (seed_offset, seed_len as u32)
            })
        } else {
            None
        };
        if let Some((seed_offset, seed_length)) = sentinel_tail_seed {
            if seed_length > 0 {
                warn!(
                    path = %normalized_path,
                    byte_offset,
                    requested_length = length,
                    seed_offset,
                    seed_length,
                    "projfs.get_file_data sentinel offset=0; pre-seeding tail before primary segment service"
                );
                match self
                    .mount_runtime
                    .read_bytes(handle.handle_id, handle.inode, seed_offset, seed_length)
                    .await
                {
                    Ok(bytes) if !bytes.is_empty() => {
                        let seed_buffer = unsafe {
                            PrjAllocateAlignedBuffer(request.namespace_context, bytes.len())
                        };
                        if !seed_buffer.is_null() {
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    bytes.as_ptr(),
                                    seed_buffer.cast::<u8>(),
                                    bytes.len(),
                                );
                            }
                            let seed_hr = unsafe {
                                PrjWriteFileData(
                                    request.namespace_context,
                                    &request.data_stream_id,
                                    seed_buffer,
                                    seed_offset,
                                    bytes.len() as u32,
                                )
                            };
                            unsafe {
                                PrjFreeAlignedBuffer(seed_buffer);
                            }
                            if seed_hr != S_OK {
                                warn!(
                                    path = %normalized_path,
                                    seed_offset,
                                    seed_length = bytes.len(),
                                    hr = format_hresult(seed_hr),
                                    "projfs.get_file_data sentinel tail seed write failed"
                                );
                            } else {
                                warn!(
                                    path = %normalized_path,
                                    seed_offset,
                                    seed_length = bytes.len(),
                                    "projfs.get_file_data sentinel tail seed write completed"
                                );
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(
                            path = %normalized_path,
                            seed_offset,
                            seed_length,
                            ?error,
                            "projfs.get_file_data sentinel tail seed read failed"
                        );
                    }
                }
            }
        }
        if is_sentinel_length {
            let capped = remaining.min(WINDOWS_PROJFS_SENTINEL_MAX_SERVICE_BYTES);
            if capped < remaining {
                warn!(
                    path = %normalized_path,
                    byte_offset,
                    requested_length = length,
                    clamped_length = capped,
                    known_file_size = handle.size_bytes,
                    "projfs.get_file_data detected sentinel callback length; capping per-command service window"
                );
            }
            remaining = capped;
            warn!(
                path = %normalized_path,
                byte_offset,
                requested_length = length,
                serviced_length = remaining,
                known_file_size = handle.size_bytes,
                "projfs.get_file_data detected sentinel callback length"
            );
        }
        let mut total_delivered = 0usize;
        let mut sampled_header = false;
        let mut hr = S_OK;

        while remaining > 0 {
            let first_huge_request = total_delivered == 0
                && byte_offset == 0
                && length >= WINDOWS_PROJFS_LARGE_REQUEST_THRESHOLD_BYTES;
            let target_chunk_bytes = if first_huge_request {
                WINDOWS_PROJFS_BOOTSTRAP_CHUNK_BYTES
            } else {
                callback_chunk_bytes
            };
            let request_length = remaining.min(u64::from(target_chunk_bytes)) as u32;
            let mut read_offset = next_offset;
            let mut read_length = request_length;
            let mut leading_aligned_bytes = 0u32;
            if !next_offset.is_multiple_of(WINDOWS_PROJFS_WRITE_ALIGNMENT_BYTES) {
                let aligned_offset = next_offset & !(WINDOWS_PROJFS_WRITE_ALIGNMENT_BYTES - 1);
                let prefix = (next_offset - aligned_offset) as u32;
                read_offset = aligned_offset;
                read_length = request_length.saturating_add(prefix);
                leading_aligned_bytes = prefix;
            }
            let read_result = self
                .mount_runtime
                .read_bytes(handle.handle_id, handle.inode, read_offset, read_length)
                .await;

            let bytes = match read_result {
                Ok(bytes) => bytes,
                Err(error) => {
                    let result = if matches!(error, MountRuntimeError::StaleLease { .. }) {
                        "estale"
                    } else {
                        "error"
                    };
                    let elapsed = started_at.elapsed();
                    warn!(
                        path = %normalized_path,
                        byte_offset = read_offset,
                        requested_length = read_length,
                        full_requested_length = length,
                        ?error,
                        "projfs.get_file_data read failed"
                    );
                    record_windows_projfs_callback(result);
                    record_windows_projfs_callback_duration(elapsed, result);
                    warn_if_slow_callback(normalized_path, byte_offset, length, elapsed, result);
                    return hresult_from_mount_error(error);
                }
            };

            if !sampled_header && byte_offset == 0 && !bytes.is_empty() {
                let header_len = bytes.len().min(16);
                let header_prefix = bytes[..header_len]
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                warn!(
                    path = %normalized_path,
                    byte_offset,
                    requested_length = length,
                    delivered_length = bytes.len(),
                    header_prefix = %header_prefix,
                    "projfs.get_file_data header sample"
                );
                sampled_header = true;
            }

            if bytes.is_empty() {
                break;
            }

            let buffer =
                unsafe { PrjAllocateAlignedBuffer(request.namespace_context, bytes.len()) };
            if buffer.is_null() {
                let elapsed = started_at.elapsed();
                record_windows_projfs_callback("error");
                record_windows_projfs_callback_duration(elapsed, "error");
                warn_if_slow_callback(normalized_path, byte_offset, length, elapsed, "error");
                return hresult_from_win32(ERROR_NOT_ENOUGH_MEMORY);
            }

            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast::<u8>(), bytes.len());
            }

            hr = unsafe {
                PrjWriteFileData(
                    request.namespace_context,
                    &request.data_stream_id,
                    buffer,
                    read_offset,
                    bytes.len() as u32,
                )
            };
            unsafe {
                PrjFreeAlignedBuffer(buffer);
            }

            if hr != S_OK {
                if hr == hresult_from_win32(ERROR_INVALID_HANDLE) {
                    warn!(
                        path = %normalized_path,
                        byte_offset = next_offset,
                        requested_length = request_length,
                        delivered_length = total_delivered,
                        full_requested_length = length,
                        "projfs.get_file_data stream handle closed while callback was still writing"
                    );
                    hr = S_OK;
                    remaining = 0;
                    break;
                }
                let write_end_exclusive = next_offset.saturating_add(bytes.len() as u64);
                let write_past_known_size = handle
                    .size_bytes
                    .map(|size_bytes| write_end_exclusive > size_bytes)
                    .unwrap_or(false);
                warn!(
                    path = %normalized_path,
                    byte_offset = read_offset,
                    requested_length = read_length,
                    delivered_length = bytes.len(),
                    full_requested_length = length,
                    offset_aligned_4k = read_offset.is_multiple_of(4096),
                    length_aligned_4k = bytes.len().is_multiple_of(4096),
                    write_end_exclusive,
                    known_file_size = handle.size_bytes,
                    write_past_known_size,
                    hr = format_hresult(hr),
                    "projfs.get_file_data write failed"
                );
                break;
            }

            let delivered = bytes.len() as u64;
            let consumed = delivered.saturating_sub(u64::from(leading_aligned_bytes));
            if consumed == 0 {
                warn!(
                    path = %normalized_path,
                    byte_offset = read_offset,
                    requested_length = read_length,
                    delivered_length = bytes.len(),
                    leading_aligned_bytes,
                    full_requested_length = length,
                    "projfs.get_file_data consumed zero bytes after alignment adjustment"
                );
                hr = hresult_from_win32(ERROR_INVALID_PARAMETER);
                break;
            }

            total_delivered += consumed as usize;
            if consumed >= remaining {
                remaining = 0;
            } else {
                remaining -= consumed;
                next_offset = next_offset.saturating_add(consumed);
            }
        }

        if hr == S_OK && remaining > 0 {
            let elapsed = started_at.elapsed();
            warn!(
                path = %normalized_path,
                byte_offset,
                requested_length = length,
                delivered_length = total_delivered,
                remaining_length = remaining,
                "projfs.get_file_data returned short data for requested range"
            );
            record_windows_projfs_callback("error");
            record_windows_projfs_callback_duration(elapsed, "error");
            warn_if_slow_callback(normalized_path, byte_offset, length, elapsed, "error");
            return hresult_from_win32(ERROR_INVALID_PARAMETER);
        }

        let result = if hr == S_OK { "ok" } else { "error" };
        let elapsed = started_at.elapsed();
        if elapsed > Duration::from_millis(3000)
            || total_delivered > WINDOWS_PROJFS_BOOTSTRAP_CHUNK_BYTES as usize
        {
            warn!(
                path = %normalized_path,
                byte_offset,
                requested_length = length,
                delivered_length = total_delivered,
                elapsed_ms = elapsed.as_secs_f64() * 1000.0,
                command_id = request.command_id,
                "projfs.get_file_data completed"
            );
        }
        record_windows_projfs_callback(result);
        record_windows_projfs_callback_duration(elapsed, result);
        warn_if_slow_callback(normalized_path, byte_offset, length, elapsed, result);
        hr
    }

    fn query_file_name(&self, normalized_path: &str) -> HRESULT {
        match self.mount_runtime.getattr(normalized_path) {
            Ok(_) => S_OK,
            Err(error) => {
                warn!(path = %normalized_path, ?error, "projfs.query_file_name failed");
                hresult_from_mount_error(error)
            }
        }
    }

    fn handle_notification(
        &self,
        callback_data: &PRJ_CALLBACK_DATA,
        is_directory: bool,
        notification: PRJ_NOTIFICATION,
    ) -> HRESULT {
        if is_directory {
            return S_OK;
        }

        record_windows_projfs_notification(notification_name(notification));
        match notification {
            PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED => {
                self.release_stream_handle(&callback_data.DataStreamId);
                S_OK
            }
            _ => S_OK,
        }
    }

    fn shutdown(&self) {
        self.enumerations.clear();
        self.release_all_stream_handles();
    }

    fn build_directory_entries(
        &self,
        directory_path: &str,
        search_expression: Option<&str>,
    ) -> Result<Vec<ProjfsDirectoryEntry>, MountRuntimeError> {
        let mut entries = self.mount_runtime.readdir(directory_path)?;
        entries.sort_by_cached_key(|entry| entry.name.to_ascii_lowercase());
        let entries: Vec<ProjfsDirectoryEntry> = entries
            .into_iter()
            .filter(|entry| entry.name != "." && entry.name != "..")
            .filter(|entry| matches_search_expression(&entry.name, search_expression))
            .map(|entry| ProjfsDirectoryEntry {
                name: entry.name,
                basic_info: PRJ_FILE_BASIC_INFO {
                    IsDirectory: entry.kind == MountNodeKind::Directory,
                    FileSize: i64::try_from(entry.size_bytes).unwrap_or(i64::MAX),
                    CreationTime: current_filetime(),
                    LastAccessTime: current_filetime(),
                    LastWriteTime: current_filetime(),
                    ChangeTime: current_filetime(),
                    FileAttributes: file_attributes_from_kind(entry.kind),
                },
            })
            .collect();

        if directory_path == "/" && entries.is_empty() {
            warn!(
                directory_path,
                search_expression = search_expression.unwrap_or("*"),
                "projfs.get_directory_enumeration produced no root entries"
            );
        }

        Ok(entries)
    }

    fn get_or_create_stream_handle(
        &self,
        stream_id: &GUID,
        normalized_path: &str,
    ) -> Result<MountHandle, MountRuntimeError> {
        let stream_key = Self::stream_handle_key(stream_id, normalized_path);
        if let Some(existing) = self.stream_handles.get(&stream_key) {
            record_windows_projfs_stream_handle_event("reused");
            return Ok(existing.clone());
        }

        let opened = self.mount_runtime.open(normalized_path)?;
        match self.stream_handles.entry(stream_key) {
            Entry::Occupied(existing) => {
                let _ = self.mount_runtime.release(opened.handle_id);
                record_windows_projfs_stream_handle_event("reused_race");
                Ok(existing.get().clone())
            }
            Entry::Vacant(vacant) => {
                vacant.insert(opened.clone());
                self.mount_runtime
                    .spawn_startup_prefetch(&opened, &self.runtime_handle);
                record_windows_projfs_stream_handle_event("opened");
                Ok(opened)
            }
        }
    }

    fn schedule_tail_prefetch(&self, normalized_path: &str, handle: &MountHandle) {
        if !is_tail_prefetch_candidate_path(normalized_path) {
            return;
        }
        let Some(file_size) = handle.size_bytes else {
            return;
        };
        if file_size == 0 {
            return;
        }
        if self
            .tail_prefetch_started
            .insert(normalized_path.to_owned(), ())
            .is_some()
        {
            return;
        }

        let prefetch_len = u64::from(WINDOWS_PROJFS_TAIL_PREFETCH_BYTES).min(file_size);
        let prefetch_offset = file_size.saturating_sub(prefetch_len);
        let mount_runtime = Arc::clone(&self.mount_runtime);
        let normalized_path = normalized_path.to_owned();
        let handle = handle.clone();
        self.runtime_handle.spawn(async move {
            let started_at = Instant::now();
            match mount_runtime
                .read_bytes(
                    handle.handle_id,
                    handle.inode,
                    prefetch_offset,
                    prefetch_len as u32,
                )
                .await
            {
                Ok(bytes) => {
                    info!(
                        path = %normalized_path,
                        prefetch_offset,
                        requested_bytes = prefetch_len,
                        prefetched_bytes = bytes.len(),
                        elapsed_ms = started_at.elapsed().as_secs_f64() * 1000.0,
                        "projfs.tail_prefetch completed"
                    );
                }
                Err(error) => {
                    warn!(
                        path = %normalized_path,
                        prefetch_offset,
                        requested_bytes = prefetch_len,
                        ?error,
                        "projfs.tail_prefetch failed"
                    );
                }
            }
        });
    }

    fn release_stream_handle(&self, stream_id: &GUID) {
        let stream_guid = GuidKey::from_guid(stream_id);
        let keys_to_remove: Vec<StreamHandleKey> = self
            .stream_handles
            .iter()
            .filter_map(|entry| {
                if entry.key().stream_id == stream_guid {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();
        for key in keys_to_remove {
            if let Some((_, handle)) = self.stream_handles.remove(&key) {
                let _ = self.mount_runtime.release(handle.handle_id);
                record_windows_projfs_stream_handle_event("released");
            }
        }
    }

    fn release_all_stream_handles(&self) {
        let stream_ids: Vec<StreamHandleKey> = self
            .stream_handles
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for stream_id in stream_ids {
            if let Some((_, handle)) = self.stream_handles.remove(&stream_id) {
                let _ = self.mount_runtime.release(handle.handle_id);
                record_windows_projfs_stream_handle_event("released_on_shutdown");
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct StreamHandleKey {
    stream_id: GuidKey,
    normalized_path: String,
}

impl MountRuntime {
    pub async fn mount_projfs_filesystem<P: AsRef<Path>>(
        self: Arc<Self>,
        mount_path: P,
        service_name: &str,
        _allow_other: bool,
    ) -> std::io::Result<WindowsProjfsMountedFilesystem> {
        let mount_path = mount_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&mount_path)?;
        let metadata = std::fs::metadata(&mount_path)?;
        if !metadata.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("mountpoint {} is not a directory", mount_path.display()),
            ));
        }

        let instance = Arc::new(WindowsProjfsInstance::new(
            mount_path.clone(),
            self,
            Handle::current(),
            service_name.to_owned(),
        ));
        let notification_mappings = if instance.notification_mappings.is_empty() {
            null_mut()
        } else {
            instance.notification_mappings.as_ptr().cast_mut()
        };
        let notification_mappings_count =
            u32::try_from(instance.notification_mappings.len()).unwrap_or(u32::MAX);
        let instance_ptr = Arc::as_ptr(&instance);
        let root_path = widestring_os(mount_path.as_os_str());
        let root_version = placeholder_version(service_name, service_name);
        let instance_guid = stable_guid_from_seed(service_name);

        let mark_result = unsafe {
            PrjMarkDirectoryAsPlaceholder(root_path.as_ptr(), null(), &root_version, &instance_guid)
        };
        if mark_result != S_OK {
            warn!(
                hr = format_hresult(mark_result),
                mountpoint = %mount_path.display(),
                "ProjFS root placeholder mark did not succeed; continuing to virtualization start"
            );
        }

        let options = PRJ_STARTVIRTUALIZING_OPTIONS {
            Flags: PRJ_FLAG_NONE,
            PoolThreadCount: 0,
            ConcurrentThreadCount: 0,
            NotificationMappings: notification_mappings,
            NotificationMappingsCount: notification_mappings_count,
        };
        let mut namespace_context = null_mut();
        let start_result = unsafe {
            PrjStartVirtualizing(
                root_path.as_ptr(),
                &PROJFS_CALLBACKS,
                instance_ptr.cast::<c_void>().cast_mut(),
                &options,
                &mut namespace_context,
            )
        };

        if start_result != S_OK {
            return Err(io_error_from_hresult(
                start_result,
                "failed to start ProjFS virtualization",
            ));
        }

        Ok(WindowsProjfsMountedFilesystem {
            mount_path,
            namespace_context,
            instance,
        })
    }
}

unsafe extern "system" fn start_directory_enumeration_callback(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const GUID,
) -> HRESULT {
    let Ok(instance) = instance_from_callback_data(callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    let Ok(directory_path) = normalized_path_from_callback_data(instance, callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    let Some(enumeration_id) = enumeration_id.as_ref() else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    instance.start_directory_enumeration(enumeration_id, &directory_path)
}

unsafe extern "system" fn end_directory_enumeration_callback(
    _callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const GUID,
) -> HRESULT {
    let Ok(instance) = instance_from_callback_data(_callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    let Some(enumeration_id) = enumeration_id.as_ref() else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    instance.end_directory_enumeration(enumeration_id)
}

unsafe extern "system" fn get_directory_enumeration_callback(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const GUID,
    search_expression: PCWSTR,
    dir_entry_buffer_handle: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    let Ok(instance) = instance_from_callback_data(callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    let Ok(directory_path) = normalized_path_from_callback_data(instance, callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    let Some(enumeration_id) = enumeration_id.as_ref() else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    let restart_scan = ((*callback_data).Flags & PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN) != 0;
    let search_expression = normalize_callback_search_expression(search_expression);
    instance.get_directory_enumeration(
        &directory_path,
        enumeration_id,
        search_expression.as_deref(),
        restart_scan,
        dir_entry_buffer_handle,
    )
}

unsafe extern "system" fn get_placeholder_info_callback(
    callback_data: *const PRJ_CALLBACK_DATA,
) -> HRESULT {
    let Ok(instance) = instance_from_callback_data(callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    let Ok(normalized_path) = normalized_path_from_callback_data(instance, callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    instance.get_placeholder_info(&*callback_data, &normalized_path)
}

unsafe extern "system" fn get_file_data_callback(
    callback_data: *const PRJ_CALLBACK_DATA,
    byte_offset: u64,
    length: u32,
) -> HRESULT {
    let Ok(instance) = clone_instance_from_callback_data(callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    let Ok(normalized_path) = normalized_path_from_callback_data(&instance, callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    instance.queue_get_file_data(&*callback_data, &normalized_path, byte_offset, length)
}

unsafe extern "system" fn query_file_name_callback(
    callback_data: *const PRJ_CALLBACK_DATA,
) -> HRESULT {
    let Ok(instance) = instance_from_callback_data(callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    let Ok(normalized_path) = normalized_path_from_callback_data(instance, callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    instance.query_file_name(&normalized_path)
}

unsafe extern "system" fn notification_callback(
    callback_data: *const PRJ_CALLBACK_DATA,
    is_directory: bool,
    notification: PRJ_NOTIFICATION,
    _destination_filename: PCWSTR,
    _operation_parameters: *mut windows_sys::Win32::Storage::ProjectedFileSystem::PRJ_NOTIFICATION_PARAMETERS,
) -> HRESULT {
    let Ok(instance) = instance_from_callback_data(callback_data) else {
        return hresult_from_win32(ERROR_INVALID_PARAMETER);
    };
    instance.handle_notification(&*callback_data, is_directory, notification)
}

fn instance_from_callback_data(
    callback_data: *const PRJ_CALLBACK_DATA,
) -> Result<&'static WindowsProjfsInstance, HRESULT> {
    if callback_data.is_null() {
        return Err(hresult_from_win32(ERROR_INVALID_PARAMETER));
    }
    let instance_ptr = unsafe {
        (*callback_data)
            .InstanceContext
            .cast::<WindowsProjfsInstance>()
    };
    if instance_ptr.is_null() {
        return Err(hresult_from_win32(ERROR_INVALID_PARAMETER));
    }
    Ok(unsafe { &*instance_ptr })
}

fn clone_instance_from_callback_data(
    callback_data: *const PRJ_CALLBACK_DATA,
) -> Result<Arc<WindowsProjfsInstance>, HRESULT> {
    if callback_data.is_null() {
        return Err(hresult_from_win32(ERROR_INVALID_PARAMETER));
    }
    let instance_ptr = unsafe {
        (*callback_data)
            .InstanceContext
            .cast::<WindowsProjfsInstance>()
    };
    if instance_ptr.is_null() {
        return Err(hresult_from_win32(ERROR_INVALID_PARAMETER));
    }
    unsafe {
        Arc::increment_strong_count(instance_ptr);
        Ok(Arc::from_raw(instance_ptr))
    }
}

fn normalized_path_from_callback_data(
    instance: &WindowsProjfsInstance,
    callback_data: *const PRJ_CALLBACK_DATA,
) -> Result<String, HRESULT> {
    if callback_data.is_null() {
        return Err(hresult_from_win32(ERROR_INVALID_PARAMETER));
    }
    let raw_path = unsafe { pcwstr_to_string((*callback_data).FilePathName) };
    Ok(instance.normalize_callback_path(&raw_path))
}

fn strip_stream_suffix(path: &str) -> &str {
    let file_name = path.rsplit('/').next().unwrap_or(path);
    let Some(stream_index) = file_name.find(':') else {
        return path;
    };
    let suffix_offset = file_name.len().saturating_sub(stream_index);
    &path[..path.len().saturating_sub(suffix_offset)]
}

fn normalize_callback_search_expression(raw: PCWSTR) -> Option<String> {
    if raw.is_null() {
        return None;
    }
    let value = pcwstr_to_string(raw);
    normalize_search_expression(Some(value.as_str()))
}

fn normalize_search_expression(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty() && *value != "*")
        .map(ToOwned::to_owned)
}

fn matches_search_expression(name: &str, expression: Option<&str>) -> bool {
    let Some(expression) = expression else {
        return true;
    };
    let name_wide = widestring(name);
    let expression_wide = widestring(expression);
    unsafe { PrjFileNameMatch(name_wide.as_ptr(), expression_wide.as_ptr()) }
}

fn file_basic_info_from_attributes(
    attributes: &crate::mount::MountAttributes,
) -> PRJ_FILE_BASIC_INFO {
    PRJ_FILE_BASIC_INFO {
        IsDirectory: attributes.kind == MountNodeKind::Directory,
        FileSize: i64::try_from(attributes.size_bytes).unwrap_or(i64::MAX),
        CreationTime: current_filetime(),
        LastAccessTime: current_filetime(),
        LastWriteTime: current_filetime(),
        ChangeTime: current_filetime(),
        FileAttributes: file_attributes_from_kind(attributes.kind),
    }
}

fn file_attributes_from_kind(kind: MountNodeKind) -> u32 {
    match kind {
        MountNodeKind::Directory => FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_READONLY,
        MountNodeKind::File => FILE_ATTRIBUTE_READONLY,
    }
}

fn notification_name(notification: PRJ_NOTIFICATION) -> &'static str {
    match notification {
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION => "file_handle_closed_no_modification",
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED => "file_handle_closed_file_modified",
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED => "file_handle_closed_file_deleted",
        _ => "other",
    }
}

fn warn_if_slow_callback(
    path: &str,
    byte_offset: u64,
    length: u32,
    elapsed: std::time::Duration,
    result: &'static str,
) {
    let elapsed_ms = elapsed.as_millis();
    if elapsed_ms < WINDOWS_PROJFS_SLOW_CALLBACK_WARN_MS {
        return;
    }

    warn!(
        path = %path,
        byte_offset,
        length,
        elapsed_ms,
        result,
        "slow Windows ProjFS GetFileData callback"
    );
}

fn warn_if_slow_projfs_operation(
    operation: &'static str,
    path: &str,
    elapsed: std::time::Duration,
    hr: Option<HRESULT>,
) {
    let elapsed_ms = elapsed.as_millis();
    if elapsed_ms < WINDOWS_PROJFS_SLOW_CALLBACK_WARN_MS {
        return;
    }

    match hr {
        Some(hr) => warn!(
            operation,
            path = %path,
            elapsed_ms,
            hr = format_hresult(hr),
            "slow Windows ProjFS callback"
        ),
        None => warn!(
            operation,
            path = %path,
            elapsed_ms,
            "slow Windows ProjFS callback"
        ),
    }
}

fn current_filetime() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = duration.as_secs().saturating_add(WINDOWS_EPOCH_OFFSET_SECS);
    let ticks = seconds
        .saturating_mul(10_000_000)
        .saturating_add(u64::from(duration.subsec_nanos() / 100));
    i64::try_from(ticks).unwrap_or(i64::MAX)
}

fn stable_guid_from_seed(seed: &str) -> GUID {
    let mut bytes = [0u8; 16];
    let seed_bytes = seed.as_bytes();
    for (index, byte) in bytes.iter_mut().enumerate() {
        let source = seed_bytes
            .get(index % seed_bytes.len())
            .copied()
            .unwrap_or(0);
        *byte = source ^ ((index as u8).wrapping_mul(31));
    }
    GUID {
        data1: u32::from_le_bytes(bytes[0..4].try_into().expect("slice length is fixed")),
        data2: u16::from_le_bytes(bytes[4..6].try_into().expect("slice length is fixed")),
        data3: u16::from_le_bytes(bytes[6..8].try_into().expect("slice length is fixed")),
        data4: bytes[8..16].try_into().expect("slice length is fixed"),
    }
}

fn placeholder_version(provider_seed: &str, content_seed: &str) -> PRJ_PLACEHOLDER_VERSION_INFO {
    PRJ_PLACEHOLDER_VERSION_INFO {
        ProviderID: fixed_identifier(provider_seed),
        ContentID: fixed_identifier(content_seed),
    }
}

fn placeholder_info_for_path(
    provider_seed: &str,
    content_seed: &str,
    file_basic_info: PRJ_FILE_BASIC_INFO,
) -> PRJ_PLACEHOLDER_INFO {
    PRJ_PLACEHOLDER_INFO {
        FileBasicInfo: file_basic_info,
        VersionInfo: placeholder_version(provider_seed, content_seed),
        ..Default::default()
    }
}

fn fixed_identifier(seed: &str) -> [u8; 128] {
    let mut buffer = [0u8; 128];
    let seed_bytes = seed.as_bytes();
    for (index, byte) in seed_bytes.iter().copied().enumerate().take(buffer.len()) {
        buffer[index] = byte;
    }
    buffer
}

fn widestring(value: &str) -> Vec<u16> {
    widestring_os(OsStr::new(value))
}

fn widestring_os(value: &OsStr) -> Vec<u16> {
    value.encode_wide().chain(Some(0)).collect()
}

fn pcwstr_to_string(value: PCWSTR) -> String {
    if value.is_null() {
        return String::new();
    }

    let mut length = 0usize;
    unsafe {
        while *value.add(length) != 0 {
            length += 1;
        }
        String::from_utf16_lossy(std::slice::from_raw_parts(value, length))
    }
}

fn is_tail_prefetch_candidate_path(path: &str) -> bool {
    path.rsplit_once('.')
        .map(|(_, extension)| extension.eq_ignore_ascii_case("mp4"))
        .unwrap_or(false)
}

fn hresult_from_mount_error(error: MountRuntimeError) -> HRESULT {
    match error {
        MountRuntimeError::PathNotFound { .. }
        | MountRuntimeError::InodeNotFound { .. }
        | MountRuntimeError::HandleNotFound { .. } => hresult_from_win32(ERROR_FILE_NOT_FOUND),
        MountRuntimeError::NotDirectory { .. } => hresult_from_win32(ERROR_DIRECTORY),
        MountRuntimeError::NotFile { .. } | MountRuntimeError::MissingUrl { .. } => {
            hresult_from_win32(ERROR_PATH_NOT_FOUND)
        }
        MountRuntimeError::InvalidName { .. } | MountRuntimeError::HandleInodeMismatch { .. } => {
            hresult_from_win32(ERROR_INVALID_PARAMETER)
        }
        MountRuntimeError::StaleLease { .. } | MountRuntimeError::Io { .. } => {
            hresult_from_win32(ERROR_NOT_SUPPORTED)
        }
        MountRuntimeError::MissingDetails { .. } | MountRuntimeError::ShuttingDown => {
            hresult_from_win32(ERROR_NOT_SUPPORTED)
        }
    }
}

fn hresult_from_win32(code: u32) -> HRESULT {
    if code == 0 {
        return S_OK;
    }
    ((code & 0x0000_FFFF) | (7 << 16) | 0x8000_0000) as i32
}

fn io_error_from_hresult(hr: HRESULT, context: &str) -> std::io::Error {
    std::io::Error::other(format!("{context}: {}", format_hresult(hr)))
}

fn format_hresult(hr: HRESULT) -> String {
    format!("0x{:08X}", hr as u32)
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_search_expression, stable_guid_from_seed, GuidKey, WindowsProjfsInstance,
    };
    use std::path::PathBuf;
    use std::sync::Arc;

    use tokio::runtime::Handle;

    use crate::{
        catalog::state::CatalogStateStore,
        mount::MountRuntime,
        proto::{
            catalog_entry::Details as CatalogEntryDetails, CatalogEntry, CatalogEntryKind,
            CatalogFileTransport, CatalogLeaseState, CatalogLocatorSource, CatalogMatchBasis,
            CatalogMediaType, CatalogPlaybackRole, CatalogProviderFamily, CatalogSnapshot,
            DirectoryEntry, FileEntry,
        },
    };

    #[test]
    fn normalizes_windows_callback_paths() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should build");
        let instance = WindowsProjfsInstance::new(
            PathBuf::from(r"E:\FilmuVFS"),
            Arc::new(MountRuntime::new(
                Arc::new(CatalogStateStore::new()),
                "session".to_owned(),
            )),
            runtime.handle().clone(),
            "filmuvfs".to_owned(),
        );
        assert_eq!(instance.normalize_callback_path(""), "/");
        assert_eq!(
            instance.normalize_callback_path(r"movies\Fight Club.mkv"),
            "/movies/Fight Club.mkv"
        );
        assert_eq!(
            instance.normalize_callback_path(r"\shows\Dir\"),
            "/shows/Dir"
        );
        assert_eq!(
            instance.normalize_callback_path(r"E:\FilmuVFS\movies\Fight Club.mkv"),
            "/movies/Fight Club.mkv"
        );
        assert_eq!(
            instance.normalize_callback_path(r"E:\FilmuVFS\movies\Fight Club.mkv::$DATA"),
            "/movies/Fight Club.mkv"
        );
    }

    #[test]
    fn collapses_empty_search_expressions() {
        assert_eq!(normalize_search_expression(None), None);
        assert_eq!(normalize_search_expression(Some("*")), None);
        assert_eq!(
            normalize_search_expression(Some("*.mkv")).as_deref(),
            Some("*.mkv")
        );
    }

    #[test]
    fn derives_stable_guid_keys() {
        let guid = stable_guid_from_seed("filmuvfs");
        assert_eq!(GuidKey::from_guid(&guid), GuidKey::from_guid(&guid));
    }

    #[tokio::test]
    async fn reuses_stream_handles_per_data_stream_id() {
        let state = seeded_state();
        let runtime = Arc::new(MountRuntime::new(state, "session".to_owned()));
        let instance = WindowsProjfsInstance::new(
            PathBuf::from(r"E:\FilmuVFS"),
            Arc::clone(&runtime),
            Handle::current(),
            "filmuvfs".to_owned(),
        );
        let stream_id = stable_guid_from_seed("stream-1");

        let first = instance
            .get_or_create_stream_handle(
                &stream_id,
                "/movies/Example Movie (2024)/Example Movie (2024).mkv",
            )
            .expect("first handle should open");
        let second = instance
            .get_or_create_stream_handle(
                &stream_id,
                "/movies/Example Movie (2024)/Example Movie (2024).mkv",
            )
            .expect("second handle should reuse the first stream handle");

        assert_eq!(first.handle_id, second.handle_id);
        assert_eq!(runtime.open_handle_count(), 1);

        instance.release_stream_handle(&stream_id);
        assert_eq!(runtime.open_handle_count(), 0);
    }

    #[tokio::test]
    async fn shutdown_releases_all_cached_stream_handles() {
        let state = seeded_state();
        let runtime = Arc::new(MountRuntime::new(state, "session".to_owned()));
        let instance = WindowsProjfsInstance::new(
            PathBuf::from(r"E:\FilmuVFS"),
            Arc::clone(&runtime),
            Handle::current(),
            "filmuvfs".to_owned(),
        );

        instance
            .get_or_create_stream_handle(
                &stable_guid_from_seed("stream-1"),
                "/movies/Example Movie (2024)/Example Movie (2024).mkv",
            )
            .expect("movie handle should open");
        instance
            .get_or_create_stream_handle(
                &stable_guid_from_seed("stream-2"),
                "/shows/Example Show/S01/E01.mkv",
            )
            .expect("episode handle should open");

        assert_eq!(runtime.open_handle_count(), 2);
        instance.shutdown();
        assert_eq!(runtime.open_handle_count(), 0);
    }

    #[tokio::test]
    async fn does_not_reuse_stream_handle_across_different_paths() {
        let state = seeded_state();
        let runtime = Arc::new(MountRuntime::new(state, "session".to_owned()));
        let instance = WindowsProjfsInstance::new(
            PathBuf::from(r"E:\FilmuVFS"),
            Arc::clone(&runtime),
            Handle::current(),
            "filmuvfs".to_owned(),
        );
        let stream_id = stable_guid_from_seed("shared-stream-id");

        let movie_handle = instance
            .get_or_create_stream_handle(
                &stream_id,
                "/movies/Example Movie (2024)/Example Movie (2024).mkv",
            )
            .expect("movie handle should open");
        let episode_handle = instance
            .get_or_create_stream_handle(&stream_id, "/shows/Example Show/S01/E01.mkv")
            .expect("episode handle should open even with same stream id");

        assert_ne!(
            movie_handle.handle_id, episode_handle.handle_id,
            "stream handle cache must be path-aware"
        );
        assert_eq!(runtime.open_handle_count(), 2);

        instance.release_stream_handle(&stream_id);
        assert_eq!(runtime.open_handle_count(), 0);
    }

    fn seeded_state() -> Arc<CatalogStateStore> {
        let state = Arc::new(CatalogStateStore::new());
        state
            .apply_snapshot(sample_catalog_snapshot())
            .expect("sample snapshot should apply");
        state
    }

    fn sample_catalog_snapshot() -> CatalogSnapshot {
        CatalogSnapshot {
            generation_id: "generation-1".to_owned(),
            entries: vec![
                directory_entry("dir:/", None, "/", "/"),
                directory_entry("dir:/movies", Some("dir:/"), "/movies", "movies"),
                directory_entry("dir:/shows", Some("dir:/"), "/shows", "shows"),
                directory_entry(
                    "dir:/movies/Example Movie (2024)",
                    Some("dir:/movies"),
                    "/movies/Example Movie (2024)",
                    "Example Movie (2024)",
                ),
                directory_entry(
                    "dir:/shows/Example Show",
                    Some("dir:/shows"),
                    "/shows/Example Show",
                    "Example Show",
                ),
                directory_entry(
                    "dir:/shows/Example Show/S01",
                    Some("dir:/shows/Example Show"),
                    "/shows/Example Show/S01",
                    "S01",
                ),
                file_entry(
                    "file:movie-1",
                    "dir:/movies/Example Movie (2024)",
                    "/movies/Example Movie (2024)/Example Movie (2024).mkv",
                    "Example Movie (2024).mkv",
                    CatalogMediaType::Movie,
                    "http://127.0.0.1:18080/movie.mkv",
                    "provider-file-movie-1",
                ),
                file_entry(
                    "file:episode-1",
                    "dir:/shows/Example Show/S01",
                    "/shows/Example Show/S01/E01.mkv",
                    "E01.mkv",
                    CatalogMediaType::Episode,
                    "http://127.0.0.1:18080/episode.mkv",
                    "provider-file-episode-1",
                ),
            ],
            stats: None,
        }
    }

    fn directory_entry(
        entry_id: &str,
        parent_entry_id: Option<&str>,
        path: &str,
        name: &str,
    ) -> CatalogEntry {
        CatalogEntry {
            entry_id: entry_id.to_owned(),
            parent_entry_id: parent_entry_id.map(str::to_owned),
            path: path.to_owned(),
            name: name.to_owned(),
            kind: CatalogEntryKind::Directory as i32,
            correlation: None,
            details: Some(CatalogEntryDetails::Directory(DirectoryEntry {})),
        }
    }

    fn file_entry(
        entry_id: &str,
        parent_entry_id: &str,
        path: &str,
        name: &str,
        media_type: CatalogMediaType,
        unrestricted_url: &str,
        provider_file_id: &str,
    ) -> CatalogEntry {
        CatalogEntry {
            entry_id: entry_id.to_owned(),
            parent_entry_id: Some(parent_entry_id.to_owned()),
            path: path.to_owned(),
            name: name.to_owned(),
            kind: CatalogEntryKind::File as i32,
            correlation: None,
            details: Some(CatalogEntryDetails::File(FileEntry {
                item_id: format!("item:{entry_id}"),
                item_title: name.to_owned(),
                item_external_ref: Some(format!("ext:{entry_id}")),
                media_entry_id: format!("media:{entry_id}"),
                source_attachment_id: None,
                media_type: media_type as i32,
                transport: CatalogFileTransport::RemoteDirect as i32,
                locator: unrestricted_url.to_owned(),
                local_path: None,
                restricted_url: None,
                unrestricted_url: Some(unrestricted_url.to_owned()),
                original_filename: Some(name.to_owned()),
                size_bytes: Some(1_024),
                lease_state: CatalogLeaseState::Ready as i32,
                expires_at: None,
                last_refreshed_at: None,
                last_refresh_error: None,
                provider: Some("realdebrid".to_owned()),
                provider_download_id: Some(format!("download:{entry_id}")),
                provider_file_id: Some(provider_file_id.to_owned()),
                provider_file_path: Some(format!("Media/{name}")),
                active_roles: vec![CatalogPlaybackRole::Direct as i32],
                source_key: Some(format!("source:{entry_id}")),
                query_strategy: Some("by-media-entry-id".to_owned()),
                provider_family: CatalogProviderFamily::Debrid as i32,
                locator_source: CatalogLocatorSource::UnrestrictedUrl as i32,
                match_basis: CatalogMatchBasis::ProviderFileId as i32,
                restricted_fallback: false,
            })),
        }
    }
}
