use axum::body::Body;
use axum::extract::Request;
use axum::http::HeaderMap;

pub use crate::otel_http::TRACE_HEADER_NAMES;

/// Forward end-to-end request headers. JSON framing and trace propagation are
/// owned by the caller; a configured backend key replaces caller Authorization.
pub fn forward_request_headers(
    mut request: reqwest::RequestBuilder,
    headers: Option<&HeaderMap>,
    override_authorization: bool,
    override_dp_rank: bool,
) -> reqwest::RequestBuilder {
    if let Some(headers) = headers {
        let connection_headers: Vec<String> = headers
            .get_all("connection")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .map(|name| name.trim().to_ascii_lowercase())
            .collect();
        for (name, value) in headers {
            let key = name.as_str();
            if matches!(
                key,
                "host"
                    | "content-type"
                    | "content-length"
                    | "content-encoding"
                    | "connection"
                    | "keep-alive"
                    | "proxy-authenticate"
                    | "proxy-authorization"
                    | "proxy-connection"
                    | "te"
                    | "trailer"
                    | "transfer-encoding"
                    | "upgrade"
            ) || TRACE_HEADER_NAMES.contains(&key)
                || connection_headers.iter().any(|header| header == key)
                || (override_authorization && key == "authorization")
                || (override_dp_rank && key == "x-data-parallel-rank")
            {
                continue;
            }
            request = request.header(name, value);
        }
    }
    request
}

/// Copy request headers to a Vec of name-value string pairs
/// Used for forwarding headers to backend workers
pub fn copy_request_headers(req: &Request<Body>) -> Vec<(String, String)> {
    req.headers()
        .iter()
        .filter_map(|(name, value)| {
            // Convert header value to string, skipping non-UTF8 headers
            value
                .to_str()
                .ok()
                .map(|v| (name.to_string(), v.to_string()))
        })
        .collect()
}

/// Convert headers from reqwest Response to axum HeaderMap
/// Filters out hop-by-hop headers that shouldn't be forwarded
pub fn preserve_response_headers(reqwest_headers: &HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let connection_headers: Vec<_> = reqwest_headers
        .get_all("connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();

    for (name, value) in reqwest_headers.iter() {
        // Skip hop-by-hop headers that shouldn't be forwarded
        let name_str = name.as_str().to_lowercase();
        if should_forward_header(&name_str)
            && !connection_headers.iter().any(|header| header == &name_str)
        {
            // The original name and value are already valid, so we can just clone them
            headers.append(name.clone(), value.clone());
        }
    }

    headers
}

/// Determine if a header should be forwarded from backend to client
fn should_forward_header(name: &str) -> bool {
    // List of headers that should NOT be forwarded (hop-by-hop headers)
    !matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "host" // Should not forward the backend's host header
    )
}

/// Propagate OpenTelemetry trace headers to a reqwest RequestBuilder
///
/// When OTel is enabled: actively injects the current span's trace context,
/// making the router's span the parent of the backend request's span.
/// When OTel is disabled: passively forwards existing trace headers from
/// the incoming request.
pub fn propagate_trace_headers(
    request: reqwest::RequestBuilder,
    headers: Option<&HeaderMap>,
) -> reqwest::RequestBuilder {
    crate::otel_http::propagate_trace_headers(request, headers)
}

/// Propagate specific headers from incoming request to outgoing reqwest RequestBuilder
///
/// This is a general-purpose helper for selectively forwarding headers by name.
/// Only headers whose names match the provided list (case-insensitive) are propagated.
///
/// # Arguments
/// * `request` - The reqwest RequestBuilder to add headers to
/// * `headers` - Optional incoming headers to check
/// * `header_names` - List of header names to propagate (matched case-insensitively)
///
/// # Returns
/// The RequestBuilder with matching headers added
pub fn propagate_headers(
    mut request: reqwest::RequestBuilder,
    headers: Option<&HeaderMap>,
    header_names: &[&str],
) -> reqwest::RequestBuilder {
    if let Some(h) = headers {
        for &name in header_names {
            if let Some(value) = h.get(name) {
                request = request.header(name, value);
            }
        }
    }
    request
}

#[cfg(test)]
mod forwarding_tests {
    use super::*;

    #[test]
    fn preserves_encoding_and_repeated_headers_filters_connection_options() {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "gzip".parse().unwrap());
        headers.insert("connection", "X-Private".parse().unwrap());
        headers.insert("x-private", "internal".parse().unwrap());
        headers.insert("trailer", "x-checksum".parse().unwrap());
        headers.append("set-cookie", "a=1".parse().unwrap());
        headers.append("set-cookie", "b=2".parse().unwrap());
        let forwarded = preserve_response_headers(&headers);
        // Encoded bytes remain encoded; reqwest removes this itself if decoding.
        assert_eq!(forwarded["content-encoding"], "gzip");
        assert_eq!(forwarded.get_all("set-cookie").iter().count(), 2);
        for key in ["connection", "x-private", "trailer"] {
            assert!(!forwarded.contains_key(key));
        }
    }

    #[test]
    fn forwards_api_headers_but_not_framing_or_connection_headers() {
        let mut headers = HeaderMap::new();
        for (key, value) in [
            ("host", "ingress.example"),
            ("content-length", "999"),
            ("content-encoding", "gzip"),
            ("connection", "x-private, keep-alive"),
            ("x-private", "internal"),
            ("transfer-encoding", "chunked"),
            ("anthropic-version", "2023-06-01"),
            ("anthropic-beta", "example"),
            ("x-api-key", "caller-key"),
            ("authorization", "Bearer caller"),
            ("traceparent", "trace"),
            ("x-data-parallel-rank", "999"),
        ] {
            headers.insert(key, value.parse().unwrap());
        }
        let client = reqwest::Client::new();
        let request = forward_request_headers(
            client
                .post("http://localhost/test")
                .json(&serde_json::json!({})),
            Some(&headers),
            true,
            true,
        )
        .header("authorization", "Bearer backend")
        .build()
        .unwrap();
        let forwarded = request.headers();
        for key in [
            "host",
            "connection",
            "x-private",
            "transfer-encoding",
            "content-encoding",
            "traceparent",
            "x-data-parallel-rank",
        ] {
            assert!(!forwarded.contains_key(key), "{key}");
        }
        assert_ne!(
            forwarded
                .get("content-length")
                .and_then(|v| v.to_str().ok()),
            Some("999")
        );
        assert_eq!(forwarded["anthropic-version"], "2023-06-01");
        assert_eq!(forwarded["anthropic-beta"], "example");
        assert_eq!(forwarded["x-api-key"], "caller-key");
        assert_eq!(forwarded.get_all("authorization").iter().count(), 1);
        assert_eq!(forwarded["authorization"], "Bearer backend");
        let caller = forward_request_headers(
            client.post("http://localhost/test"),
            Some(&headers),
            false,
            false,
        )
        .build()
        .unwrap();
        assert_eq!(caller.headers()["authorization"], "Bearer caller");
        assert_eq!(caller.headers()["x-data-parallel-rank"], "999");
    }
}
