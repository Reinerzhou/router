use std::fs;
use tracing::Level;
use vllm_router_rs::logging::{init_logging, LoggingConfig};

#[test]
fn lifecycle_jsonl_preserves_span_fields_independently_of_native_level() {
    let root = tempfile::tempdir().unwrap();
    std::env::set_var("OBSERVABILITY_ENABLED", "1");
    std::env::set_var("OBSERVABILITY_LOG_DIR", root.path());
    std::env::set_var("RUST_LOG", "error");
    std::env::set_var("OBSERVABILITY_ROLE", "span-fixture");
    std::env::remove_var("OBSERVABILITY_ALERT_CONFIG");
    std::env::remove_var("OBSERVABILITY_WEBHOOK_URL_FILE");
    let guard = init_logging(
        LoggingConfig {
            level: Level::ERROR,
            ..LoggingConfig::default()
        },
        None,
    );
    {
        let span = tracing::info_span!(
            target: "vllm_router_rs::observability",
            "request_scope", correlation = "span-request", worker_rank = 3u64
        );
        let _entered = span.enter();
        tracing::info!(
            target: "vllm_router_rs::observability",
            event = "span_fixture", request_id = "span-request", detail = "preserved"
        );
        tracing::error!(target: "vllm_router_rs::server", "native-output-fixture");
    }
    drop(guard);
    let component = root.path().join("router");
    let node = fs::read_dir(&component)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let role = node.join("span-fixture");
    let rows: Vec<serde_json::Value> = fs::read_dir(role)
        .unwrap()
        .map(|entry| fs::read_to_string(entry.unwrap().path()).unwrap())
        .flat_map(|text| {
            text.lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    let event = rows
        .iter()
        .find(|row| row["event"] == "span_fixture")
        .unwrap();
    assert_eq!(event["request_id"], "span-request");
    assert_eq!(event["detail"], "preserved");
    assert_eq!(event["span"]["correlation"], "span-request");
    assert_eq!(event["span"]["worker_rank"], 3);
    assert_eq!(event["spans"][0]["name"], "request_scope");
    assert!(event["timestamp"].is_string());
    assert!(event["timestamp_unix_ns"].is_i64());
    assert!(!rows
        .iter()
        .any(|row| row["message"] == "native-output-fixture"));
    assert!(!root.path().join("console").exists());
}
