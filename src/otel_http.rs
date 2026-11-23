use axum::http::HeaderMap;
use opentelemetry::trace::TraceContextExt;
use tracing::{field::Empty, info_span, instrument::WithSubscriber, Dispatch, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use url::Url;

pub const TRACE_HEADER_NAMES: &[&str] = &["traceparent", "tracestate", "baggage"];

#[derive(Clone, Copy, Debug)]
pub struct ClientRequestOptions<'a> {
    pub method: &'a str,
    pub url: &'a str,
    pub route: Option<&'a str>,
    pub request_phase: Option<&'a str>,
}

pub struct PreparedClientRequest {
    request: reqwest::RequestBuilder,
    span: Span,
    observation: crate::observability::ClientObservation,
}

impl PreparedClientRequest {
    pub async fn send(self) -> Result<reqwest::Response, reqwest::Error> {
        let Self {
            request,
            span,
            observation,
        } = self;

        let response = if crate::otel_trace::is_otel_enabled() {
            // Propagation headers are already set. Hyper's keep-alive tasks can
            // outlive this request and must not retain its client or server span.
            request.send().with_subscriber(Dispatch::none()).await
        } else {
            request.send().await
        };

        match response {
            Ok(response) => {
                observation.finish(Some(response.status().as_u16()), false);
                if !span.is_disabled() {
                    let status = response.status();
                    span.record("http.response.status_code", i64::from(status.as_u16()));

                    if status.is_server_error() {
                        span.set_status(opentelemetry::trace::Status::error(format!(
                            "HTTP {}",
                            status.as_u16()
                        )));
                    }
                }

                Ok(response)
            }
            Err(error) => {
                observation.finish(None, true);
                if !span.is_disabled() {
                    let message = "backend request failed";
                    span.record("error", true);
                    span.set_status(opentelemetry::trace::Status::error(message));
                }

                Err(error)
            }
        }
    }
}

pub fn prepare_client_request(
    mut request: reqwest::RequestBuilder,
    incoming_headers: Option<&HeaderMap>,
    options: ClientRequestOptions<'_>,
) -> PreparedClientRequest {
    let (prepared, observation) = crate::observability::client_request(
        request,
        incoming_headers,
        options.request_phase,
        options.url,
    );
    request = prepared;
    // In LMDeploy inference traffic the worker and its phases share model.router as parent.
    let span = if crate::otel_trace::is_otel_enabled()
        && !(crate::otel_trace::is_lmdeploy_router()
            && crate::otel_trace::is_inference_post(options.method, options.route))
    {
        let parent = Span::current().context();
        let upstream = incoming_headers
            .filter(|_| !parent.span().span_context().is_valid())
            .map(|headers| {
                opentelemetry::global::get_text_map_propagator(|propagator| {
                    propagator.extract_with_context(
                        &opentelemetry::Context::new(),
                        &crate::otel_trace::TraceHeaders::new(headers),
                    )
                })
            });
        // Only static inference routes belong in trace metadata. Other backend paths can
        // contain user-supplied response IDs or query parameters.
        let route = options.route.filter(|route| {
            matches!(
                *route,
                "/v1/chat/completions"
                    | "/v1/completions"
                    | "/generate"
                    | "/v1/messages"
                    | "/v1/models"
                    | "/v1/responses"
                    | "/v1/embeddings"
                    | "/rerank"
                    | "/v1/rerank"
            )
        });
        let span = info_span!(
            target: "otel_trace",
            "http_client_request",
            otel.kind = "client",
            otel.name = %format_args!(
                "{} {}",
                options.method,
                route.unwrap_or("backend")
            ),
            http.request.method = %options.method,
            http.route = Empty,
            request.id = Empty,
            session.id = Empty,
            server.address = Empty,
            server.port = Empty,
            vllm.request_phase = Empty,
            http.response.status_code = Empty,
            error = Empty,
        );

        if let Some(upstream) = upstream {
            span.set_parent(upstream);
        }

        if let Some(route) = route {
            span.record("http.route", route);
        }
        if let Some(request_phase) = options.request_phase {
            span.record("vllm.request_phase", request_phase);
        }
        if let Some(headers) = incoming_headers {
            crate::otel_trace::record_correlation(&span, headers);
        }
        record_server_address(&span, options.url);

        {
            let _enter = span.enter();
            request = propagate_trace_headers(request, incoming_headers);
        }
        span
    } else {
        request = propagate_trace_headers(request, incoming_headers);
        Span::none()
    };

    PreparedClientRequest {
        request,
        span,
        observation,
    }
}

pub async fn send_client_request(
    request: reqwest::RequestBuilder,
    incoming_headers: Option<&HeaderMap>,
    options: ClientRequestOptions<'_>,
) -> Result<reqwest::Response, reqwest::Error> {
    prepare_client_request(request, incoming_headers, options)
        .send()
        .await
}

pub fn propagate_trace_headers(
    mut request: reqwest::RequestBuilder,
    headers: Option<&HeaderMap>,
) -> reqwest::RequestBuilder {
    if crate::otel_trace::is_otel_enabled() {
        let mut trace_headers = HeaderMap::new();
        crate::otel_trace::inject_request_context(&mut trace_headers, headers);
        for (name, value) in trace_headers.iter() {
            request = request.header(name, value);
        }
        return request;
    }

    crate::routers::header_utils::propagate_headers(request, headers, TRACE_HEADER_NAMES)
}

fn record_server_address(span: &Span, url: &str) {
    let Ok(parsed) = Url::parse(url) else {
        return;
    };

    if let Some(host) = parsed.host_str() {
        span.record("server.address", host);
    }

    if let Some(port) = parsed.port_or_known_default() {
        span.record("server.port", port);
    }
}

#[cfg(all(test, feature = "cpu", feature = "ready"))]
mod tests {
    use super::*;

    #[test]
    fn passive_trace_propagation_honors_connection_options() {
        // The passive path only runs when OTel is off. The enabled flag latches
        // on first use, so a developer with OTEL_* exported in their shell would
        // otherwise see this fail confusingly instead of being skipped.
        assert!(!crate::otel_trace::is_otel_enabled());
        let mut headers = HeaderMap::new();
        headers.append("connection", "keep-alive".parse().unwrap());
        headers.append("connection", " TraceParent , baggage".parse().unwrap());
        headers.insert(
            "traceparent",
            "00-11111111111111111111111111111111-2222222222222222-01"
                .parse()
                .unwrap(),
        );
        headers.insert("baggage", "private=value".parse().unwrap());
        headers.insert("tracestate", "vendor=value".parse().unwrap());
        let request = propagate_trace_headers(
            reqwest::Client::new().get("https://worker.example/"),
            Some(&headers),
        )
        .build()
        .unwrap();
        assert!(!request.headers().contains_key("traceparent"));
        assert!(!request.headers().contains_key("baggage"));
        assert_eq!(request.headers()["tracestate"], "vendor=value");
    }
}
