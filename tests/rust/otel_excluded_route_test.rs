mod common;

use axum::http::{Request, StatusCode};
use common::mock_worker::{self, MockWorker, MockWorkerConfig};
use common::test_app::create_test_app;
use opentelemetry::trace::{SpanKind, TracerProvider as _};
use opentelemetry_sdk::{testing::trace::InMemorySpanExporter, trace::TracerProvider};
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;
use vllm_router_rs::{
    config::{PolicyConfig, RouterConfig, RoutingMode},
    otel_trace::{self, PreparedOtel},
    routers::RouterFactory,
};

#[tokio::test(flavor = "current_thread")]
async fn excluded_route_preserves_upstream_trace_and_sampling() {
    std::env::set_var("OTEL_SERVICE_NAME", "lmdeploy-router");
    let exporter = InMemorySpanExporter::default();
    let provider = TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("excluded-route");
    let prepared = PreparedOtel::from_parts_with_excluded_paths(
        provider.clone(),
        tracer,
        vec!["/health_generate".into()],
    );
    let subscriber = tracing_subscriber::registry().with(prepared.layer());
    let _guard = tracing::subscriber::set_default(subscriber);
    otel_trace::activate_otel(prepared);

    let mut worker = MockWorker::new(MockWorkerConfig::default());
    let url = worker.start().await.unwrap();
    let port = url.rsplit(':').next().unwrap().parse().unwrap();
    let config = RouterConfig {
        mode: RoutingMode::Regular {
            worker_urls: vec![url.clone()],
        },
        policy: PolicyConfig::RoundRobin,
        host: "127.0.0.1".into(),
        port: 0,
        ..Default::default()
    };
    let ctx = common::create_test_context(config.clone());
    let router = RouterFactory::create_regular_router(std::slice::from_ref(&url), &ctx)
        .await
        .unwrap();
    let app = create_test_app(
        std::sync::Arc::from(router),
        reqwest::Client::new(),
        &config,
    );
    let trace = "baf7651916cd43dd8448eb211c80319c";
    let parent = "e7ad6b7169203331";
    for flags in ["00", "01"] {
        mock_worker::clear_captured_requests(port);
        exporter.reset();
        let response = app
            .clone()
            .oneshot(
                Request::get("/health_generate")
                    .header("traceparent", format!("00-{trace}-{parent}-{flags}"))
                    .header("tracestate", "a=1,b=2,c=3,d=4,e=5,f=6,g=7,h=8,i=9")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        provider.force_flush();
        let requests = mock_worker::get_captured_requests(port);
        let request = requests
            .iter()
            .find(|r| r.path == "/health_generate")
            .unwrap();
        let received: Vec<_> = request.headers["traceparent"].split('-').collect();
        assert_eq!(received[1], trace);
        assert_eq!(received[3], flags);
        assert_eq!(
            request.headers["tracestate"],
            "a=1,b=2,c=3,d=4,e=5,f=6,g=7,h=8"
        );
        let spans = exporter.get_finished_spans().unwrap();
        if flags == "00" {
            assert!(spans.is_empty());
        } else {
            let client = spans
                .iter()
                .find(|s| s.span_kind == SpanKind::Client)
                .unwrap();
            assert_eq!(client.parent_span_id.to_string(), parent);
            assert_eq!(client.span_context.span_id().to_string(), received[2]);
            assert!(!spans.iter().any(|s| s.span_kind == SpanKind::Server));
        }
    }
    worker.stop().await;
    otel_trace::shutdown_otel();
}
