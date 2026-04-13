#[allow(clippy::all, unreachable_pub)]
pub mod filmu {
    pub mod vfs {
        pub mod catalog {
            pub mod v1 {
                tonic::include_proto!("filmu.vfs.catalog.v1");
            }
        }
    }
}

pub use filmu::vfs::catalog::v1::*;

// IMPORTANT:
// These binding-guard tests are part of the pre-mount validation gate for FilmuVFS.
// Whenever the protobuf contract adds new cross-process observability or correlation
// fields (for example lease-expiry, chunk-hint, or prefetch-priority correlation data),
// extend the assertions below in the same change. The goal is to make proto evolution
// fail fast at the Rust code boundary instead of relying on docs-only discipline.
#[cfg(test)]
mod tests {
    use super::{
        watch_catalog_request, CatalogAck, CatalogCorrelationKeys, CatalogHeartbeat,
        CatalogSubscribe, FileEntry, RefreshCatalogEntryRequest, RefreshCatalogEntryResponse,
        WatchCatalogRequest,
    };

    #[test]
    fn correlation_keys_bindings_expose_required_cross_process_fields() {
        let keys = CatalogCorrelationKeys {
            item_id: Some("item-1".to_owned()),
            media_entry_id: Some("media-entry-1".to_owned()),
            source_attachment_id: Some("attachment-1".to_owned()),
            provider: Some("realdebrid".to_owned()),
            provider_download_id: Some("download-1".to_owned()),
            provider_file_id: Some("provider-file-1".to_owned()),
            provider_file_path: Some("Movies/Title.mkv".to_owned()),
            session_id: Some("session-1".to_owned()),
            handle_key: Some("handle-1".to_owned()),
            tenant_id: Some("tenant-1".to_owned()),
        };

        assert_eq!(keys.provider_file_id.as_deref(), Some("provider-file-1"));
        assert_eq!(keys.provider_file_path.as_deref(), Some("Movies/Title.mkv"));
        assert_eq!(keys.session_id.as_deref(), Some("session-1"));
        assert_eq!(keys.handle_key.as_deref(), Some("handle-1"));
        assert_eq!(keys.tenant_id.as_deref(), Some("tenant-1"));
    }

    #[test]
    fn catalog_file_entry_bindings_expose_provider_identity_fields() {
        let entry = FileEntry {
            item_id: "item-1".to_owned(),
            item_title: "Title".to_owned(),
            item_external_ref: Some("ext-1".to_owned()),
            media_entry_id: "media-entry-1".to_owned(),
            source_attachment_id: Some("attachment-1".to_owned()),
            media_type: 1,
            transport: 2,
            locator: "https://cdn.example.com/title".to_owned(),
            local_path: None,
            restricted_url: Some("https://api.example.com/title".to_owned()),
            unrestricted_url: Some("https://cdn.example.com/title".to_owned()),
            original_filename: Some("Title.mkv".to_owned()),
            size_bytes: Some(100),
            lease_state: 1,
            expires_at: None,
            last_refreshed_at: None,
            last_refresh_error: None,
            provider: Some("realdebrid".to_owned()),
            provider_download_id: Some("download-1".to_owned()),
            provider_file_id: Some("provider-file-1".to_owned()),
            provider_file_path: Some("Movies/Title.mkv".to_owned()),
            active_roles: vec![1],
            source_key: Some("media-entry".to_owned()),
            query_strategy: Some("by-media-entry-id".to_owned()),
            provider_family: 2,
            locator_source: 2,
            match_basis: 2,
            restricted_fallback: false,
        };

        assert_eq!(entry.provider_download_id.as_deref(), Some("download-1"));
        assert_eq!(entry.provider_file_id.as_deref(), Some("provider-file-1"));
        assert_eq!(
            entry.provider_file_path.as_deref(),
            Some("Movies/Title.mkv")
        );
    }

    #[test]
    fn watch_catalog_request_bindings_expose_subscribe_ack_and_heartbeat_variants() {
        let correlation = Some(CatalogCorrelationKeys::default());
        let subscribe = WatchCatalogRequest {
            command: Some(watch_catalog_request::Command::Subscribe(
                CatalogSubscribe {
                    daemon_id: "daemon-1".to_owned(),
                    daemon_version: Some("0.1.0".to_owned()),
                    last_applied_generation_id: Some("generation-1".to_owned()),
                    want_full_snapshot: true,
                    correlation: correlation.clone(),
                },
            )),
        };
        let ack = WatchCatalogRequest {
            command: Some(watch_catalog_request::Command::Ack(CatalogAck {
                event_id: "event-1".to_owned(),
                generation_id: Some("generation-2".to_owned()),
                correlation: correlation.clone(),
            })),
        };
        let heartbeat = WatchCatalogRequest {
            command: Some(watch_catalog_request::Command::Heartbeat(
                CatalogHeartbeat { correlation },
            )),
        };

        assert!(matches!(
            subscribe.command,
            Some(watch_catalog_request::Command::Subscribe(_))
        ));
        assert!(matches!(
            ack.command,
            Some(watch_catalog_request::Command::Ack(_))
        ));
        assert!(matches!(
            heartbeat.command,
            Some(watch_catalog_request::Command::Heartbeat(_))
        ));
    }

    #[test]
    fn refresh_catalog_entry_bindings_expose_inline_refresh_messages() {
        let request = RefreshCatalogEntryRequest {
            provider_file_id: "provider-file-1".to_owned(),
            handle_key: "handle-1".to_owned(),
            entry_id: "file:entry-1".to_owned(),
        };
        let response = RefreshCatalogEntryResponse {
            success: true,
            new_url: "https://cdn.example.com/fresh".to_owned(),
        };

        assert_eq!(request.provider_file_id, "provider-file-1");
        assert_eq!(request.handle_key, "handle-1");
        assert_eq!(request.entry_id, "file:entry-1");
        assert!(response.success);
        assert_eq!(response.new_url, "https://cdn.example.com/fresh");
    }
}
