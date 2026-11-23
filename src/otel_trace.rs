//! OpenTelemetry tracing integration.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, OnceLock,
    },
    time::Duration,
};

use anyhow::Result;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::{
    global, propagation::TextMapCompositePropagator, trace::TracerProvider as _, Key, KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::{BaggagePropagator, TraceContextPropagator},
    runtime,
    trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, Tracer as SdkTracer, TracerProvider},
    Resource,
};
use tracing::{Metadata, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    layer::{Context, Filter},
    Layer,
};

use crate::config::TraceConfig;

/// Whether OpenTelemetry tracing is enabled.
///
/// This flag guards runtime code paths (make_span parent extraction,
/// inject_trace_context_http). Only set to true after both the OTel layer
/// and subscriber are installed via `activate_otel`.
static ENABLED: AtomicBool = AtomicBool::new(false);
static LMDEPLOY_ROUTER: AtomicBool = AtomicBool::new(false);
static PROVIDER: Mutex<Option<TracerProvider>> = Mutex::new(None);
static EXCLUDED_PATHS: OnceLock<Box<[String]>> = OnceLock::new();

const OTEL_SPAN_TARGET: &str = "otel_trace";

/// Filter that only allows specific module targets to be exported to OTEL.
#[derive(Clone, Copy, Default)]
pub(crate) struct CustomOtelFilter;

impl CustomOtelFilter {
    #[inline]
    pub const fn new() -> Self {
        Self
    }

    #[inline]
    fn is_allowed(target: &str) -> bool {
        target.starts_with(OTEL_SPAN_TARGET)
    }
}

impl<S> Filter<S> for CustomOtelFilter
where
    S: Subscriber,
{
    #[inline]
    fn enabled(&self, meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        Self::is_allowed(meta.target())
    }

    #[inline]
    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        if Self::is_allowed(meta.target()) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }
}

/// Prepared OTel state ready to be activated.
///
/// Built by `prepare_otel()`, this holds the provider and tracer without
/// mutating any global state. Call `layer()` to create the tracing layer,
/// then `activate_otel()` after the subscriber is installed.
pub struct PreparedOtel {
    provider: TracerProvider,
    tracer: SdkTracer,
    lmdeploy_router: bool,
    excluded_paths: Box<[String]>,
}

impl PreparedOtel {
    /// Build from pre-existing parts (useful for tests with in-memory exporters).
    pub fn from_parts(provider: TracerProvider, tracer: SdkTracer) -> Self {
        Self::from_parts_with_excluded_paths(provider, tracer, Vec::new())
    }

    /// Build from pre-existing parts with an explicit excluded-path list.
    pub fn from_parts_with_excluded_paths(
        provider: TracerProvider,
        tracer: SdkTracer,
        excluded_paths: Vec<String>,
    ) -> Self {
        Self {
            provider,
            tracer,
            lmdeploy_router: is_lmdeploy_resource(&Resource::default()),
            excluded_paths: excluded_paths.into_boxed_slice(),
        }
    }

    /// Create the tracing-opentelemetry layer for this prepared state.
    pub fn layer<S>(&self) -> Box<dyn Layer<S> + Send + Sync + 'static>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
    {
        let layer = tracing_opentelemetry::layer()
            .with_tracer(self.tracer.clone())
            .with_location(false)
            .with_threads(false)
            .with_tracked_inactivity(false)
            .with_filter(CustomOtelFilter::new());
        Box::new(layer)
    }
}

/// Keep operator-supplied identity. On Kubernetes, distinguish processes sharing a pod.
fn service_instance_id(resource: &Resource) -> String {
    let attr = |key| {
        resource
            .get(Key::new(key))
            .map(|v| v.to_string())
            .filter(|v| safe_metadata(v))
    };
    if let Some(id) = attr("service.instance.id") {
        return id;
    }
    let pod = attr("k8s.pod.uid").or_else(|| {
        Some(format!(
            "{}/{}/{}",
            attr("k8s.cluster.name")?,
            attr("k8s.namespace.name")?,
            attr("k8s.pod.name")?
        ))
    });
    pod.map(|pod| format!("{pod}:{}", std::process::id()))
        .filter(|id| safe_metadata(id))
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

fn parent_based_sampler(sampling_ratio: f64) -> Sampler {
    if sampling_ratio <= 0.0 {
        Sampler::ParentBased(Box::new(Sampler::AlwaysOff))
    } else if sampling_ratio >= 1.0 {
        Sampler::ParentBased(Box::new(Sampler::AlwaysOn))
    } else {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sampling_ratio)))
    }
}

// Optional vendor state is bounded at the propagation boundary.
pub(crate) struct TraceHeaders<'a> {
    headers: &'a HeaderMap,
    parent: Option<std::borrow::Cow<'a, str>>,
    state: Option<String>,
}

impl<'a> TraceHeaders<'a> {
    pub(crate) fn new(headers: &'a HeaderMap) -> Self {
        let parent = headers
            .get("traceparent")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| {
                if !s.starts_with("00-") {
                    return Some(std::borrow::Cow::Borrowed(s));
                }
                let b = s.as_bytes();
                if b.len() != 55
                    || !b.iter().enumerate().all(|(i, c)| match i {
                        2 | 35 | 52 => *c == b'-',
                        _ => c.is_ascii_digit() || (b'a'..=b'f').contains(c),
                    })
                {
                    return None;
                }
                // SDK 0.27 rejects flags such as 03 from newer SDKs. Pass it the sampled
                // bit it supports, preserving the caller's IDs and sampling decision.
                let flags = u8::from_str_radix(&s[53..], 16).ok()?;
                if flags > 1 {
                    return Some(std::borrow::Cow::Owned(format!(
                        "{}{:02x}",
                        &s[..53],
                        flags & 1
                    )));
                }
                Some(std::borrow::Cow::Borrowed(s))
            });
        let state = headers
            .get("tracestate")
            .and_then(|value| value.to_str().ok())
            .and_then(bounded_tracestate);
        Self {
            headers,
            parent,
            state,
        }
    }
}

fn bounded_tracestate(value: &str) -> Option<String> {
    let members: Vec<_> = value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if members.len() > 32 || members.iter().any(|s| !safe_metadata(s)) {
        return None;
    }
    let pairs: Option<Vec<_>> = members.iter().map(|s| s.split_once('=')).collect();
    let pairs = pairs?;
    let mut keys = std::collections::HashSet::new();
    if pairs.iter().any(|(key, _)| !keys.insert(*key)) {
        return None;
    }
    opentelemetry::trace::TraceState::from_key_value(pairs).ok()?;
    // The deep-chain contract narrows W3C's 32 entries to eight and 512 bytes.
    // Keep complete leftmost entries instead of dropping all vendor state.
    let mut state = String::new();
    for member in members.iter().take(8) {
        let separator = usize::from(!state.is_empty());
        if state.len() + separator + member.len() > 512 {
            break;
        }
        if separator != 0 {
            state.push(',');
        }
        state.push_str(member);
    }
    (!state.is_empty()).then_some(state)
}

impl opentelemetry::propagation::Extractor for TraceHeaders<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if key == "traceparent" {
            return self.parent.as_deref();
        }
        if key == "tracestate" {
            return self.state.as_deref();
        }
        self.headers.get(key)?.to_str().ok()
    }

    fn keys(&self) -> Vec<&str> {
        self.headers.keys().map(HeaderName::as_str).collect()
    }
}

fn safe_metadata(value: &str) -> bool {
    static SECRET: OnceLock<regex::Regex> = OnceLock::new();
    !value.is_empty()
        && value.len() <= 1024
        && !value.chars().any(char::is_control)
        && !SECRET
            .get_or_init(|| {
                regex::Regex::new(r"(?i)authorization:|bearer\s+|(?:^|[^a-z0-9])sk-[a-z0-9_-]+")
                    .unwrap()
            })
            .is_match(value)
}

pub(crate) fn record_correlation(span: &tracing::Span, headers: &HeaderMap) {
    let header = |key| headers.get(key).and_then(|v| v.to_str().ok());
    if let Some(id) = header("x-request-id").filter(|v| {
        (16..=64).contains(&v.len())
            && v.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            && safe_metadata(v)
    }) {
        span.record("request.id", id);
    }
    let session = header("x-session-id")
        .filter(|v| !v.is_empty())
        .or_else(|| {
            if headers
                .get("x-session-affinity")
                .is_some_and(|v| !v.is_empty())
            {
                None
            } else {
                header("x-openclaw-session-key")
            }
        });
    if let Some(id) = session.filter(|v| safe_metadata(v)) {
        span.record("session.id", id);
    }
}

fn is_lmdeploy_resource(resource: &Resource) -> bool {
    resource.get(Key::new("service.name")) == Some("lmdeploy-router".into())
}

pub(crate) fn is_lmdeploy_router() -> bool {
    LMDEPLOY_ROUTER.load(Ordering::Relaxed)
}

pub(crate) fn is_inference_post(method: &str, route: Option<&str>) -> bool {
    method == "POST"
        && matches!(
            route,
            Some(
                "/generate"
                    | "/inference/v1/generate"
                    | "/rerank"
                    | "/v1/rerank"
                    | "/v1/chat/completions"
                    | "/v1/completions"
                    | "/v1/embeddings"
                    | "/v1/messages"
                    | "/v1/responses"
            )
        )
}

fn router_resource(resource: Resource) -> Resource {
    let instance_id = service_instance_id(&resource);
    let service_name = resource
        .get(Key::new("service.name"))
        .map(|value| value.to_string())
        .filter(|value| {
            safe_metadata(value)
                && value != "unknown_service"
                && !value.starts_with("unknown_service:")
        })
        .unwrap_or_else(|| "vllm-router".to_string());

    let resource = Resource::new(
        resource
            .iter()
            .filter(|(k, v)| {
                // Environment and SDK detectors must not inject request IDs or
                // raw affinity into the exported resource. Expand only with
                // fields agreed in the platform resource contract.
                matches!(
                    k.as_str(),
                    "deployment.environment"
                        | "k8s.cluster.name"
                        | "k8s.namespace.name"
                        | "k8s.pod.name"
                        | "k8s.node.name"
                        | "inference.engine"
                        | "inference.role"
                ) && match v {
                    opentelemetry::Value::String(_)
                    | opentelemetry::Value::Bool(_)
                    | opentelemetry::Value::I64(_) => true,
                    opentelemetry::Value::F64(value) => value.is_finite(),
                    _ => false,
                } && safe_metadata(&v.to_string())
            })
            .map(|(k, v)| KeyValue::new(k.clone(), v.clone())),
    );
    let mut identity = vec![KeyValue::new("service.instance.id", instance_id)];
    if service_name == "lmdeploy-router" {
        identity.push(KeyValue::new("inference.engine", "lmdeploy"));
        identity.push(KeyValue::new("inference.role", "router"));
    }
    identity.push(KeyValue::new("service.name", service_name));
    resource.merge(&Resource::new(identity))
}

/// Build OTel provider and tracer without mutating global state.
pub fn prepare_otel(trace_config: &TraceConfig) -> Result<PreparedOtel> {
    anyhow::ensure!(
        (0.0..=1.0).contains(&trace_config.sampling_ratio),
        "OTel sampling_ratio must be within [0.0, 1.0], got {}",
        trace_config.sampling_ratio
    );

    let mut exporter_builder = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_protocol(opentelemetry_otlp::Protocol::Grpc);

    // Only set the endpoint explicitly when configured; otherwise the SDK
    // respects OTEL_EXPORTER_OTLP_ENDPOINT (defaulting to localhost:4317).
    if let Some(ep) = trace_config.otlp_traces_endpoint.as_deref() {
        let ep = if !ep.starts_with("http://") && !ep.starts_with("https://") {
            format!("http://{ep}")
        } else {
            ep.to_string()
        };
        exporter_builder = exporter_builder.with_endpoint(&ep);
    }

    let exporter = exporter_builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create OTLP exporter: {e}"))?;

    let batch_config = BatchConfigBuilder::default()
        .with_scheduled_delay(Duration::from_millis(500))
        .with_max_export_batch_size(64)
        .build();

    let span_processor = BatchSpanProcessor::builder(exporter, runtime::Tokio)
        .with_batch_config(batch_config)
        .build();

    // Filter SDK/environment resource attributes before filling Router identity.
    let resource = router_resource(Resource::default());

    let lmdeploy_router = is_lmdeploy_resource(&resource);
    let provider = TracerProvider::builder()
        .with_sampler(parent_based_sampler(trace_config.sampling_ratio))
        .with_span_processor(span_processor)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("vllm-router");

    Ok(PreparedOtel {
        provider,
        tracer,
        lmdeploy_router,
        excluded_paths: trace_config.excluded_paths.clone().into_boxed_slice(),
    })
}

#[cfg(test)]
mod tests {
    use opentelemetry::{Key, KeyValue};
    use opentelemetry_sdk::Resource;

    use super::{bounded_tracestate, router_resource};

    #[test]
    fn vendor_state_keeps_complete_entries_within_contract_limits() {
        for count in [1, 8, 9, 32] {
            let entries: Vec<_> = (0..count).map(|i| format!("v{i}=value")).collect();
            assert_eq!(
                bounded_tracestate(&entries.join(",")),
                Some(entries[..count.min(8)].join(","))
            );
        }
        let large = format!("a={},b={},c=1", "x".repeat(250), "y".repeat(250));
        assert_eq!(bounded_tracestate(&large), Some(large));
        let over = format!("a={},b={},c=longvalue", "x".repeat(250), "y".repeat(250));
        assert_eq!(
            bounded_tracestate(&over).unwrap(),
            over.rsplit_once(',').unwrap().0
        );
        for invalid in [
            "a=1,a=2".to_string(),
            "bad".into(),
            "a=Bearer token".into(),
            (0..33)
                .map(|i| format!("v{i}=1"))
                .collect::<Vec<_>>()
                .join(","),
        ] {
            assert!(bounded_tracestate(&invalid).is_none());
        }
        assert_eq!(bounded_tracestate("a=1, b=2"), Some("a=1,b=2".into()));
    }

    #[test]
    fn router_resource_keeps_only_platform_fields_without_request_metadata() {
        let keys = [
            "deployment.environment",
            "k8s.cluster.name",
            "k8s.namespace.name",
            "k8s.pod.name",
            "k8s.node.name",
        ];
        let mut attrs: Vec<_> = keys
            .iter()
            .map(|key| KeyValue::new(*key, "metadata"))
            .collect();
        attrs.extend([
            KeyValue::new("authorization", "private"),
            KeyValue::new("custom.secret", "Bearer token"),
            KeyValue::new(
                "custom.array",
                opentelemetry::Value::Array(opentelemetry::Array::I64(vec![1, 2])),
            ),
            KeyValue::new("custom.invalid_number", f64::NAN),
        ]);
        let omitted = [
            "session.affinity",
            "SESSION.AFFINITY",
            "session.affinity_hashed",
            "session.id",
            "request.id",
            "custom.metadata",
            "service.namespace",
            "deployment.environment.name",
            "k8s.pod.uid",
            "telemetry.sdk.name",
        ];
        attrs.extend(
            omitted
                .iter()
                .map(|key| KeyValue::new(*key, "private-resource-value")),
        );
        let resource = router_resource(Resource::new(attrs));
        for key in keys {
            assert_eq!(resource.get(Key::new(key)).unwrap().to_string(), "metadata");
        }
        assert!(resource.get(Key::new("authorization")).is_none());
        assert!(resource.get(Key::new("custom.secret")).is_none());
        assert!(resource.get(Key::new("custom.array")).is_none());
        assert!(resource.get(Key::new("custom.invalid_number")).is_none());
        for key in omitted {
            assert!(
                resource.get(Key::new(key)).is_none(),
                "unexpected resource field: {key}"
            );
        }
        assert_eq!(resource.iter().count(), keys.len() + 2);
    }

    #[test]
    fn router_resource_preserves_configured_service_and_role() {
        let resource = Resource::new(vec![
            KeyValue::new("service.name", "lmdeploy-router"),
            KeyValue::new("service.instance.id", "router-instance"),
            KeyValue::new("inference.engine", "lmdeploy"),
            KeyValue::new("inference.role", "router"),
        ]);

        let resource = router_resource(resource);

        assert_eq!(
            resource.get(Key::new("service.name")).unwrap().to_string(),
            "lmdeploy-router"
        );
        assert_eq!(
            resource
                .get(Key::new("inference.engine"))
                .unwrap()
                .to_string(),
            "lmdeploy"
        );
        assert_eq!(
            resource
                .get(Key::new("inference.role"))
                .unwrap()
                .to_string(),
            "router"
        );
        assert_eq!(
            resource
                .get(Key::new("service.instance.id"))
                .unwrap()
                .to_string(),
            "router-instance"
        );
    }

    #[test]
    fn instance_id_uses_pod_identity_or_a_unique_fallback() {
        let id = |res| {
            router_resource(res)
                .get(Key::new("service.instance.id"))
                .unwrap()
                .to_string()
        };
        let from_pod = Resource::new(vec![KeyValue::new("k8s.pod.uid", "pod-uid")]);
        assert_eq!(id(from_pod), format!("pod-uid:{}", std::process::id()));
        let named = Resource::new(vec![
            KeyValue::new("k8s.cluster.name", "c"),
            KeyValue::new("k8s.namespace.name", "ns"),
            KeyValue::new("k8s.pod.name", "router"),
        ]);
        assert_eq!(id(named), format!("c/ns/router:{}", std::process::id()));
        assert!(uuid::Uuid::parse_str(&id(Resource::empty())).is_ok());
    }

    #[test]
    fn router_resource_defaults_service_name_for_unconfigured_resource() {
        for service_name in [None, Some("unknown_service:vllm_router_rs")] {
            let resource = match service_name {
                Some(name) => Resource::new(vec![KeyValue::new("service.name", name)]),
                None => Resource::empty(),
            };
            let resource = router_resource(resource);

            assert_eq!(
                resource.get(Key::new("service.name")).unwrap().to_string(),
                "vllm-router"
            );
        }
    }
}

/// Set global propagator and provider, flip ENABLED.
///
/// Must be called after the subscriber with the OTel layer is installed.
pub fn activate_otel(prepared: PreparedOtel) {
    let PreparedOtel {
        provider,
        excluded_paths,
        lmdeploy_router,
        ..
    } = prepared;

    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));

    let _ = EXCLUDED_PATHS.set(excluded_paths);
    let _ = global::set_tracer_provider(provider.clone());
    *PROVIDER.lock().unwrap() = Some(provider);
    LMDEPLOY_ROUTER.store(lmdeploy_router, Ordering::Relaxed);
    ENABLED.store(true, Ordering::Release);
    tracing::info!("[tracing] OpenTelemetry tracing enabled");
}

/// Mark OpenTelemetry tracing as enabled (for tests that manage their own provider).
pub fn mark_otel_enabled() {
    LMDEPLOY_ROUTER.store(
        is_lmdeploy_resource(&Resource::default()),
        Ordering::Relaxed,
    );
    ENABLED.store(true, Ordering::Release);
}

/// Check if OpenTelemetry tracing is enabled.
#[inline]
pub fn is_otel_enabled() -> bool {
    ENABLED.load(Ordering::Acquire)
}

#[inline]
pub fn is_excluded_http_path(path: &str) -> bool {
    EXCLUDED_PATHS
        .get()
        .is_some_and(|paths| paths.iter().any(|excluded| excluded == path))
}

pub fn shutdown_otel() {
    if ENABLED.load(Ordering::Acquire) {
        tracing::info!("[tracing] OpenTelemetry shutting down");
        if let Some(provider) = PROVIDER.lock().unwrap().take() {
            let _ = provider.shutdown();
        }
        ENABLED.store(false, Ordering::Release);
    }
}

/// Inject W3C trace context headers into an HTTP request's HeaderMap.
///
/// When OTel is enabled, this injects the current span's trace context
/// (traceparent, tracestate) into outgoing requests, making the router's
/// span the parent of backend spans.
///
/// When OTel is disabled, this is a no-op.
#[inline]
pub fn inject_trace_context_http(headers: &mut HeaderMap) {
    inject_request_context(headers, None);
}

pub(crate) fn inject_request_context(headers: &mut HeaderMap, incoming: Option<&HeaderMap>) {
    use opentelemetry::trace::TraceContextExt;
    if !is_otel_enabled() {
        return;
    }

    let mut context = tracing::Span::current().context();

    struct HeaderInjector<'a>(&'a mut HeaderMap);

    impl opentelemetry::propagation::Injector for HeaderInjector<'_> {
        #[inline]
        fn set(&mut self, key: &str, value: String) {
            if let Ok(header_name) = HeaderName::from_bytes(key.as_bytes()) {
                if let Ok(header_value) = HeaderValue::from_str(&value) {
                    self.0.insert(header_name, header_value);
                }
            }
        }
    }

    global::get_text_map_propagator(|propagator| {
        let span = context.span();
        let current = span.span_context();
        let upstream = incoming
            .filter(|_| {
                !current.is_valid()
                    || (!current.is_sampled() && current.trace_state().header().is_empty())
            })
            .map(|source| {
                propagator.extract_with_context(
                    &opentelemetry::Context::new(),
                    &TraceHeaders::new(source),
                )
            });
        if let Some(upstream) = upstream {
            let parent = upstream.span();
            let parent = parent.span_context();
            if !current.is_valid() {
                // An excluded route can have no local SERVER span.
                context = upstream.clone();
            } else if parent.trace_id() == current.trace_id() {
                // tracing-opentelemetry 0.28 drops tracestate on a Drop sampling
                // decision. Preserve validated vendor state without changing IDs
                // or the upstream sampling decision.
                context = context.with_remote_span_context(opentelemetry::trace::SpanContext::new(
                    current.trace_id(),
                    current.span_id(),
                    current.trace_flags(),
                    current.is_remote(),
                    parent.trace_state().clone(),
                ));
            }
        }
        propagator.inject_context(&context, &mut HeaderInjector(headers));
    });
}
