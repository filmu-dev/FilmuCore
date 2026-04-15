use opentelemetry::{global, propagation::Injector};
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[cfg(not(target_os = "windows"))]
use hyper::http::request::Builder as HttpRequestBuilder;

pub const REQUEST_ID_HEADER: &str = "x-request-id";
pub const SESSION_ID_HEADER: &str = "x-filmu-vfs-session-id";
pub const DAEMON_ID_HEADER: &str = "x-filmu-vfs-daemon-id";
pub const ENTRY_ID_HEADER: &str = "x-filmu-vfs-entry-id";
pub const PROVIDER_FILE_ID_HEADER: &str = "x-filmu-vfs-provider-file-id";
pub const HANDLE_KEY_HEADER: &str = "x-filmu-vfs-handle-key";

struct HeaderCollector<'a> {
    headers: &'a mut Vec<(String, String)>,
}

impl Injector for HeaderCollector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.headers.push((key.to_owned(), value));
    }
}

fn propagation_headers(span: &Span) -> Vec<(String, String)> {
    let mut headers = Vec::new();
    let context = span.context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &context,
            &mut HeaderCollector {
                headers: &mut headers,
            },
        );
    });
    headers
}

fn insert_metadata_value(metadata: &mut MetadataMap, key: &str, value: &str) {
    if value.trim().is_empty() {
        return;
    }
    if let (Ok(name), Ok(metadata_value)) = (
        MetadataKey::<Ascii>::from_bytes(key.as_bytes()),
        MetadataValue::try_from(value),
    ) {
        metadata.insert(name, metadata_value);
    }
}

pub fn apply_tonic_observability_metadata(
    metadata: &mut MetadataMap,
    span: &Span,
    request_id: &str,
    daemon_id: &str,
    session_id: &str,
    extra_headers: &[(&str, &str)],
) {
    for (key, value) in propagation_headers(span) {
        insert_metadata_value(metadata, &key, &value);
    }
    insert_metadata_value(metadata, REQUEST_ID_HEADER, request_id);
    insert_metadata_value(metadata, DAEMON_ID_HEADER, daemon_id);
    insert_metadata_value(metadata, SESSION_ID_HEADER, session_id);
    for (key, value) in extra_headers {
        insert_metadata_value(metadata, key, value);
    }
}

#[cfg(not(target_os = "windows"))]
pub fn apply_http_observability_headers(
    builder: HttpRequestBuilder,
    span: &Span,
    request_id: &str,
    daemon_id: &str,
    session_id: &str,
    extra_headers: &[(&str, &str)],
) -> HttpRequestBuilder {
    let mut builder = builder;
    for (key, value) in propagation_headers(span) {
        builder = builder.header(key.as_str(), value.as_str());
    }
    builder = builder
        .header(REQUEST_ID_HEADER, request_id)
        .header(DAEMON_ID_HEADER, daemon_id)
        .header(SESSION_ID_HEADER, session_id);
    for (key, value) in extra_headers {
        if !value.trim().is_empty() {
            builder = builder.header(*key, *value);
        }
    }
    builder
}

pub fn cross_process_request_id(session_id: &str, suffix: &str) -> String {
    format!("{session_id}:{suffix}")
}

#[cfg(test)]
mod tests {
    use super::{
        apply_tonic_observability_metadata, cross_process_request_id, DAEMON_ID_HEADER,
        ENTRY_ID_HEADER, HANDLE_KEY_HEADER, PROVIDER_FILE_ID_HEADER, REQUEST_ID_HEADER,
        SESSION_ID_HEADER,
    };
    use tonic::metadata::MetadataMap;
    use tracing::info_span;

    #[test]
    fn request_id_builder_preserves_session_prefix() {
        assert_eq!(
            cross_process_request_id("session-1", "watch-catalog"),
            "session-1:watch-catalog"
        );
    }

    #[test]
    fn tonic_metadata_includes_cross_process_correlation_headers() {
        let span = info_span!("test_span");
        let mut metadata = MetadataMap::new();
        apply_tonic_observability_metadata(
            &mut metadata,
            &span,
            "req-1",
            "daemon-1",
            "session-1",
            &[
                (ENTRY_ID_HEADER, "entry-1"),
                (PROVIDER_FILE_ID_HEADER, "provider-file-1"),
                (HANDLE_KEY_HEADER, "handle-1"),
            ],
        );

        assert_eq!(
            metadata
                .get(REQUEST_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("req-1")
        );
        assert_eq!(
            metadata
                .get(DAEMON_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("daemon-1")
        );
        assert_eq!(
            metadata
                .get(SESSION_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("session-1")
        );
        assert_eq!(
            metadata
                .get(ENTRY_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("entry-1")
        );
        assert_eq!(
            metadata
                .get(PROVIDER_FILE_ID_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("provider-file-1")
        );
        assert_eq!(
            metadata
                .get(HANDLE_KEY_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("handle-1")
        );
    }
}
