use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::{
    header::{HeaderMap, RANGE, RETRY_AFTER},
    Request, StatusCode, Uri,
};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::telemetry::{record_upstream_failure, record_upstream_retryable_event};

const UPSTREAM_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const UPSTREAM_RETRY_ATTEMPTS: usize = 5;
const UPSTREAM_RETRY_BACKOFF_MS: u64 = 500;
const UPSTREAM_RETRY_BACKOFF_MAX_MS: u64 = 5_000;

#[derive(Debug, Clone, Default)]
pub struct ReadCancellation {
    handle: Option<CancellationToken>,
    external: Option<CancellationToken>,
}

impl ReadCancellation {
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn from_handle(handle: CancellationToken) -> Self {
        Self {
            handle: Some(handle),
            external: None,
        }
    }

    #[must_use]
    pub fn with_external(handle: CancellationToken, external: Option<CancellationToken>) -> Self {
        Self {
            handle: Some(handle),
            external,
        }
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
            || self
                .external
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
    }

    pub async fn cancelled(&self) {
        match (self.handle.as_ref(), self.external.as_ref()) {
            (Some(handle), Some(external)) => {
                tokio::select! {
                    _ = handle.cancelled() => {}
                    _ = external.cancelled() => {}
                }
            }
            (Some(handle), None) => {
                handle.cancelled().await;
            }
            (None, Some(external)) => {
                external.cancelled().await;
            }
            (None, None) => std::future::pending::<()>().await,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRequest {
    pub url: String,
    pub offset: u64,
    pub size: u32,
}

impl RangeRequest {
    #[must_use]
    pub fn new(url: String, offset: u64, size: u32) -> Self {
        Self { url, offset, size }
    }

    #[must_use]
    pub fn end_inclusive(&self) -> u64 {
        self.offset
            .saturating_add(u64::from(self.size).saturating_sub(1))
    }

    #[must_use]
    pub fn range_header_value(&self) -> String {
        format!("bytes={}-{}", self.offset, self.end_inclusive())
    }

    pub fn build_http_request(&self) -> Result<Request<Empty<Bytes>>, UpstreamReadError> {
        let uri = self.url.parse::<Uri>().map_err(|source| {
            record_upstream_failure("invalid_url");
            UpstreamReadError::InvalidUrl {
                url: self.url.clone(),
                source,
            }
        })?;

        Request::builder()
            .method("GET")
            .uri(uri)
            .header(RANGE, self.range_header_value())
            .body(Empty::new())
            .map_err(|error| {
                record_upstream_failure("build_request");
                UpstreamReadError::BuildRequest {
                    message: error.to_string(),
                }
            })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UpstreamReadError {
    #[error("invalid upstream URL {url}: {source}")]
    InvalidUrl {
        url: String,
        #[source]
        source: hyper::http::uri::InvalidUri,
    },
    #[error("failed to build upstream request: {message}")]
    BuildRequest { message: String },
    #[error("upstream request failed: {message}")]
    Network { message: String },
    #[error("upstream returned stale status {status}")]
    StaleStatus { status: StatusCode },
    #[error("upstream returned unexpected status {status}")]
    UnexpectedStatus { status: StatusCode },
    #[error("failed to collect upstream response body: {message}")]
    ReadBody { message: String },
    #[error("upstream read was cancelled")]
    Cancelled,
}

impl UpstreamReadError {
    #[must_use]
    pub fn is_stale(&self) -> bool {
        matches!(self, Self::StaleStatus { .. })
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    #[must_use]
    pub fn status_code(&self) -> Option<StatusCode> {
        match self {
            Self::StaleStatus { status } | Self::UnexpectedStatus { status } => Some(*status),
            _ => None,
        }
    }

    #[must_use]
    pub fn network(message: impl Into<String>) -> Self {
        Self::Network {
            message: message.into(),
        }
    }
}

#[derive(Clone)]
pub struct UpstreamReader {
    client: Client<HttpsConnector<HttpConnector>, Empty<Bytes>>,
}

impl Default for UpstreamReader {
    fn default() -> Self {
        Self::new()
    }
}

impl UpstreamReader {
    #[must_use]
    pub fn new() -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }

    pub async fn fetch_range(
        &self,
        request: RangeRequest,
        cancellation: &ReadCancellation,
    ) -> Result<Bytes, UpstreamReadError> {
        for attempt in 1..=UPSTREAM_RETRY_ATTEMPTS {
            if cancellation.is_cancelled() {
                return Err(UpstreamReadError::Cancelled);
            }
            let http_request = request.build_http_request()?;
            let response_result = tokio::select! {
                _ = cancellation.cancelled() => return Err(UpstreamReadError::Cancelled),
                result = timeout(UPSTREAM_REQUEST_TIMEOUT, self.client.request(http_request)) => result,
            };
            let response = match response_result {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    if attempt < UPSTREAM_RETRY_ATTEMPTS {
                        record_upstream_retryable_event("network");
                        tokio::select! {
                            _ = cancellation.cancelled() => return Err(UpstreamReadError::Cancelled),
                            _ = sleep(retry_backoff(attempt)) => {}
                        }
                        continue;
                    }
                    record_upstream_failure("network");
                    return Err(UpstreamReadError::Network {
                        message: error.to_string(),
                    });
                }
                Err(_) => {
                    if attempt < UPSTREAM_RETRY_ATTEMPTS {
                        record_upstream_retryable_event("network");
                        tokio::select! {
                            _ = cancellation.cancelled() => return Err(UpstreamReadError::Cancelled),
                            _ = sleep(retry_backoff(attempt)) => {}
                        }
                        continue;
                    }
                    record_upstream_failure("network");
                    return Err(UpstreamReadError::Network {
                        message: format!(
                            "request timed out after {}s",
                            UPSTREAM_REQUEST_TIMEOUT.as_secs()
                        ),
                    });
                }
            };

            let status = response.status();
            if matches!(
                status,
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::GONE
            ) {
                record_upstream_failure("stale_status");
                return Err(UpstreamReadError::StaleStatus { status });
            }

            if !status.is_success() {
                if should_retry_status(status) && attempt < UPSTREAM_RETRY_ATTEMPTS {
                    record_upstream_retryable_event(retryable_status_event(status));
                    tokio::select! {
                        _ = cancellation.cancelled() => return Err(UpstreamReadError::Cancelled),
                        _ = sleep(retry_delay_for_response(response.headers(), attempt)) => {}
                    }
                    continue;
                }
                record_upstream_failure("unexpected_status");
                if status == StatusCode::TOO_MANY_REQUESTS {
                    record_upstream_failure("unexpected_status_too_many_requests");
                } else if status.is_server_error() {
                    record_upstream_failure("unexpected_status_server_error");
                }
                return Err(UpstreamReadError::UnexpectedStatus { status });
            }

            let collected_result = tokio::select! {
                _ = cancellation.cancelled() => return Err(UpstreamReadError::Cancelled),
                result = timeout(UPSTREAM_REQUEST_TIMEOUT, response.into_body().collect()) => result,
            };
            match collected_result {
                Ok(Ok(collected)) => return Ok(collected.to_bytes()),
                Ok(Err(error)) => {
                    if attempt < UPSTREAM_RETRY_ATTEMPTS {
                        record_upstream_retryable_event("read_body");
                        tokio::select! {
                            _ = cancellation.cancelled() => return Err(UpstreamReadError::Cancelled),
                            _ = sleep(retry_backoff(attempt)) => {}
                        }
                        continue;
                    }
                    record_upstream_failure("read_body");
                    return Err(UpstreamReadError::ReadBody {
                        message: error.to_string(),
                    });
                }
                Err(_) => {
                    if attempt < UPSTREAM_RETRY_ATTEMPTS {
                        record_upstream_retryable_event("read_body");
                        tokio::select! {
                            _ = cancellation.cancelled() => return Err(UpstreamReadError::Cancelled),
                            _ = sleep(retry_backoff(attempt)) => {}
                        }
                        continue;
                    }
                    record_upstream_failure("read_body");
                    return Err(UpstreamReadError::ReadBody {
                        message: format!(
                            "body collection timed out after {}s",
                            UPSTREAM_REQUEST_TIMEOUT.as_secs()
                        ),
                    });
                }
            }
        }

        Err(UpstreamReadError::network(
            "upstream retry loop exhausted unexpectedly",
        ))
    }
}

fn retry_backoff(attempt: usize) -> Duration {
    let exponent = attempt.saturating_sub(1).min(6) as u32;
    let scaled = UPSTREAM_RETRY_BACKOFF_MS.saturating_mul(1u64 << exponent);
    Duration::from_millis(scaled.min(UPSTREAM_RETRY_BACKOFF_MAX_MS))
}

fn retryable_status_event(status: StatusCode) -> &'static str {
    if status == StatusCode::TOO_MANY_REQUESTS {
        "status_too_many_requests"
    } else {
        "status_server_error"
    }
}

fn should_retry_status(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

fn retry_delay_for_response(headers: &HeaderMap, attempt: usize) -> Duration {
    retry_after_delay(headers).unwrap_or_else(|| retry_backoff(attempt))
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?;
    let as_str = value.to_str().ok()?.trim();
    let seconds = as_str.parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_scales_and_caps() {
        assert_eq!(retry_backoff(1), Duration::from_millis(500));
        assert_eq!(retry_backoff(2), Duration::from_millis(1_000));
        assert_eq!(retry_backoff(3), Duration::from_millis(2_000));
        assert_eq!(retry_backoff(4), Duration::from_millis(4_000));
        assert_eq!(retry_backoff(5), Duration::from_millis(5_000));
        assert_eq!(retry_backoff(6), Duration::from_millis(5_000));
    }

    #[test]
    fn retry_after_header_overrides_default_backoff() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, "3".parse().expect("valid retry-after header"));

        assert_eq!(
            retry_delay_for_response(&headers, 1),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn retry_logic_includes_429() {
        assert!(should_retry_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(should_retry_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(!should_retry_status(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn retryable_status_event_classifies_provider_pressure() {
        assert_eq!(
            retryable_status_event(StatusCode::TOO_MANY_REQUESTS),
            "status_too_many_requests"
        );
        assert_eq!(
            retryable_status_event(StatusCode::BAD_GATEWAY),
            "status_server_error"
        );
    }
}
