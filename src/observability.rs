//! Native request lifecycle, rolling files and asynchronous event notifications.
//! All observations come from serving traffic; no inference probes are issued.
use axum::{
    body::{Body, Bytes},
    extract::Request,
    http::{HeaderMap, HeaderValue},
    middleware::Next,
    response::Response,
};
use base64::Engine as _;
use chrono::Utc;
use dashmap::DashMap;
use futures_util::Stream;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender},
        Arc, Mutex, OnceLock,
    },
    task::{Context, Poll},
    thread,
    time::{Duration, Instant, SystemTime},
};
use tracing::field::{Field, Visit};
use tracing_subscriber::{layer::Context as LayerContext, Layer};

const OBS_ID: &str = "x-observability-request-id";
macro_rules! native_info {
    ($($fields:tt)*) => {tracing::info!(component="router",service=%config().service,role=%config().role,node=%config().node,$($fields)*)};
}
static REQUESTS: OnceLock<DashMap<String, Arc<Mutex<RequestState>>>> = OnceLock::new();
static CONFIG: OnceLock<Config> = OnceLock::new();
static ALERTS: OnceLock<SyncSender<Value>> = OnceLock::new();
static CONDITIONS: OnceLock<Mutex<HashMap<String, HashSet<String>>>> = OnceLock::new();
static STARTED: AtomicU64 = AtomicU64::new(0);
static COMPLETED: AtomicU64 = AtomicU64::new(0);

fn number(name: &str, default: f64) -> f64 {
    let key = format!("OBSERVABILITY_{name}");
    let value = std::env::var(&key)
        .map(|s| {
            s.parse::<f64>()
                .expect("invalid numeric observability setting")
        })
        .unwrap_or(default);
    assert!(
        value.is_finite() && value > 0.,
        "{key} must be finite and positive"
    );
    value
}

pub struct Config {
    service: String,
    role: String,
    node: String,
    interval: Duration,
    retention: Duration,
    max_bytes: u64,
    e2e: f64,
    cooldown: Duration,
    webhook: Option<String>,
    feishu: bool,
    signing_secret: Option<String>,
    enabled: bool,
}

fn config() -> &'static Config {
    CONFIG.get_or_init(|| {
        let alert_file = std::env::var("OBSERVABILITY_ALERT_CONFIG")
            .ok()
            .filter(|p| !p.is_empty());
        let alert: Value = alert_file
            .as_ref()
            .map(|path| {
                serde_json::from_str(&fs::read_to_string(path).expect("cannot read alert config"))
                    .expect("invalid alert config JSON")
            })
            .unwrap_or(Value::Null);
        let webhook = if alert_file.is_some() {
            Some(
                alert["url"]
                    .as_str()
                    .expect("alert config requires url")
                    .to_owned(),
            )
        } else {
            std::env::var("OBSERVABILITY_WEBHOOK_URL_FILE")
                .ok()
                .filter(|p| !p.is_empty())
                .map(|path| {
                    fs::read_to_string(path)
                        .expect("cannot read private webhook file")
                        .trim()
                        .to_owned()
                })
        };
        if let Some(url) = &webhook {
            assert!(
                url.starts_with("http://") || url.starts_with("https://"),
                "alert URL must be HTTP(S)"
            );
        }
        let feishu = if alert_file.is_some() {
            alert["format"].as_str().unwrap_or("feishu") == "feishu"
        } else {
            webhook
                .as_ref()
                .is_some_and(|url| url.starts_with("https://open.feishu.cn/open-apis/bot/"))
        };
        let signing_secret = alert["secret"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        let e2e = alert["slow_request_seconds"].as_f64().unwrap_or(30.);
        assert!(
            e2e.is_finite() && e2e > 0.,
            "slow_request_seconds must be finite and positive"
        );
        let role = std::env::var("OBSERVABILITY_ROLE").unwrap_or_else(|_| "router".into());
        let service =
            std::env::var("OBSERVABILITY_SERVICE").unwrap_or_else(|_| "online-inference".into());
        for s in [&role, &service] {
            assert!(
                !s.is_empty()
                    && s.bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c)),
                "invalid service/role identifier"
            );
        }
        Config {
            role,
            service,
            node: std::env::var("HOSTNAME")
                .ok()
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    fs::read_to_string("/proc/sys/kernel/hostname")
                        .ok()
                        .map(|s| s.trim().to_owned())
                })
                .unwrap_or_else(|| "unknown".into()),
            interval: Duration::from_secs_f64(number("LOG_INTERVAL_S", 60.)),
            retention: Duration::from_secs_f64(number("LOG_RETENTION_DAYS", 7.) * 86400.),
            max_bytes: (number("LOG_MAX_MB", 256.) * 1024. * 1024.) as u64,
            e2e,
            cooldown: Duration::from_secs(300),
            webhook,
            feishu,
            signing_secret,
            enabled: match std::env::var("OBSERVABILITY_ENABLED").as_deref() {
                Ok("0") => false,
                Ok("1") => true,
                Ok(_) => panic!("OBSERVABILITY_ENABLED must be 0 or 1"),
                Err(_) => {
                    std::env::var("OBSERVABILITY_LOG_DIR").is_ok()
                        || alert_file.is_some()
                        || std::env::var("OBSERVABILITY_WEBHOOK_URL_FILE").is_ok()
                }
            },
        }
    })
}

fn requests() -> &'static DashMap<String, Arc<Mutex<RequestState>>> {
    REQUESTS.get_or_init(DashMap::new)
}

pub fn notify(kind: &str, severity: &str, fields: Value, status: &str) {
    let c = config();
    let Some(sender) = ALERTS.get() else { return };
    if !c.enabled || c.webhook.is_none() || !essential_alert(kind) {
        return;
    }
    let labels = json!({"alertname":kind,"severity":severity,"service":c.service,
        "component":"router","role":c.role,"node":c.node});
    let mut annotations: serde_json::Map<String, Value> = fields
        .as_object()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| {
            let mut text = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            for key in ["OPENAI_API_KEY", "VLLM_API_KEY", "HF_TOKEN"] {
                if let Ok(secret) = std::env::var(key) {
                    if !secret.is_empty() {
                        text = text.replace(&secret, "<redacted>");
                    }
                }
            }
            (key, json!(text))
        })
        .collect();
    annotations.insert(
        "summary".into(),
        json!(format!("Router {kind} on {}", c.role)),
    );
    let stamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut fingerprint = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hasher::write(&mut fingerprint, labels.to_string().as_bytes());
    let fingerprint = format!("{:016x}", std::hash::Hasher::finish(&fingerprint));
    let payload = json!({"version":"4","receiver":"native-observability","status":status,
        "groupKey":fingerprint,"commonLabels":labels,"commonAnnotations":annotations,
        "alerts":[{"status":status,"labels":labels,"annotations":annotations,"startsAt":stamp,
            "fingerprint":fingerprint,"endsAt":if status=="resolved" {stamp.as_str()} else {"0001-01-01T00:00:00Z"}}]});
    if sender.try_send(payload).is_err() {
        metrics::counter!("vllm_router_observability_alert_dropped_total").increment(1);
    }
}

fn essential_alert(kind: &str) -> bool {
    matches!(
        kind,
        "cuda_oom"
            | "gpu_fault"
            | "runtime_fatal"
            | "request_failed"
            | "request_e2e_slow"
            | "log_queue_full"
            | "log_io_failed"
    )
}

fn feishu_message(payload: &Value, secret: Option<&str>) -> Value {
    let labels = &payload["commonLabels"];
    let fields = &payload["commonAnnotations"];
    let value = |key: &str| labels[key].as_str().unwrap_or("");
    let mut text = format!(
        "[推理告警] {} {}\n状态: {}\n服务: {}\n组件: {} / {}\n节点: {}\n时间: {}",
        value("severity"),
        value("alertname"),
        payload["status"].as_str().unwrap_or(""),
        value("service"),
        value("component"),
        value("role"),
        value("node"),
        payload["alerts"][0]["startsAt"].as_str().unwrap_or("")
    );
    for key in [
        "request_id",
        "route",
        "elapsed_seconds",
        "threshold_seconds",
        "error",
        "message",
    ] {
        if let Some(value) = fields.get(key) {
            let value = value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string());
            text.push_str(&format!(
                "\n{key}: {}",
                value.chars().take(1000).collect::<String>()
            ));
        }
    }
    let mut body = json!({"msg_type":"text","content":{"text":text}});
    if let Some(secret) = secret {
        let timestamp = Utc::now().timestamp().to_string();
        let key = ring::hmac::Key::new(
            ring::hmac::HMAC_SHA256,
            format!("{timestamp}\n{secret}").as_bytes(),
        );
        body["sign"] =
            json!(base64::engine::general_purpose::STANDARD
                .encode(ring::hmac::sign(&key, b"").as_ref()));
        body["timestamp"] = json!(timestamp);
    }
    body
}

pub struct NativeGuard {
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
    _retention: Option<thread::JoinHandle<()>>,
}
impl Drop for NativeGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub fn initialize(
    directory: Option<PathBuf>,
    dropped: Option<tracing_appender::non_blocking::ErrorCounter>,
) -> NativeGuard {
    let c = config();
    if !c.enabled {
        return NativeGuard {
            stop: Arc::new(AtomicBool::new(false)),
            worker: None,
            _retention: None,
        };
    }
    let (sender, receiver) = mpsc::sync_channel::<Value>(256);
    let _ = ALERTS.set(sender);
    let stop = Arc::new(AtomicBool::new(false));
    let stopping = stop.clone();
    let retention_stop = stop.clone();
    let retention = directory.map(|directory| {
        thread::spawn(move || {
            while !retention_stop.load(Ordering::Relaxed) {
                if cleanup(&directory, config().retention).is_err() {
                    notify("log_cleanup_failed", "warning", json!({}), "firing");
                }
                for _ in 0..300 {
                    if retention_stop.load(Ordering::Relaxed) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(200));
                }
            }
        })
    });
    let worker = thread::spawn(move || {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("webhook HTTP client");
        let mut last_snapshot = Instant::now();
        let mut dedup = HashMap::<String, Instant>::new();
        let mut statuses = HashMap::<String, String>::new();
        let mut starts = HashMap::<String, String>::new();
        let mut previous_dropped = 0;
        loop {
            match receiver.recv_timeout(Duration::from_millis(200)) {
                Ok(mut payload) => {
                    let key = format!("{}:{}", payload["groupKey"], payload["status"]);
                    let group = payload["groupKey"].as_str().unwrap_or("").to_owned();
                    let status = payload["status"].as_str().unwrap_or("").to_owned();
                    let now = payload["alerts"][0]["startsAt"]
                        .as_str()
                        .unwrap_or("")
                        .to_owned();
                    let started = if status == "firing" {
                        starts.entry(group.clone()).or_insert(now).clone()
                    } else {
                        starts.remove(&group).unwrap_or(now)
                    };
                    payload["alerts"][0]["startsAt"] = json!(started);
                    if status == "resolved"
                        && statuses.get(&group).map(String::as_str) != Some("firing")
                    {
                        continue;
                    }
                    if dedup
                        .get(&key)
                        .is_none_or(|time| time.elapsed() >= c.cooldown)
                    {
                        if let Some(url) = &c.webhook {
                            for attempt in 0..3 {
                                let body = if c.feishu {
                                    feishu_message(&payload, c.signing_secret.as_deref())
                                } else {
                                    payload.clone()
                                };
                                let delivered =
                                    client.post(url).json(&body).send().is_ok_and(|response| {
                                        response.status().is_success()
                                            && (!c.feishu
                                                || response.json::<Value>().ok().is_some_and(|v| {
                                                    v.get("code")
                                                        .or_else(|| v.get("StatusCode"))
                                                        .and_then(Value::as_i64)
                                                        == Some(0)
                                                }))
                                    });
                                if delivered {
                                    metrics::counter!(
                                        "vllm_router_observability_webhook_delivered_total"
                                    )
                                    .increment(1);
                                    dedup.insert(key.clone(), Instant::now());
                                    statuses.insert(group.clone(), status.clone());
                                    break;
                                } else {
                                    metrics::counter!(
                                        "vllm_router_observability_webhook_errors_total"
                                    )
                                    .increment(1);
                                    if attempt < 2 {
                                        thread::sleep(Duration::from_millis(200));
                                    }
                                }
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
            if stopping.load(Ordering::Relaxed) {
                break;
            }
            check_inflight();
            if last_snapshot.elapsed() >= c.interval {
                last_snapshot = Instant::now();
                let lost = dropped
                    .as_ref()
                    .map(|counter| counter.dropped_lines())
                    .unwrap_or(0);
                if lost > previous_dropped {
                    metrics::counter!("vllm_router_observability_log_dropped_total")
                        .increment((lost - previous_dropped) as u64);
                    notify(
                        "log_queue_full",
                        "critical",
                        json!({"dropped":lost}),
                        "firing",
                    );
                    previous_dropped = lost;
                }
                tracing::info!(event="observability_status",component="router",service=%c.service,role=%c.role,node=%c.node,
                    inflight=requests().len(),started=STARTED.load(Ordering::Relaxed),completed=COMPLETED.load(Ordering::Relaxed),log_dropped=lost);
            }
        }
    });
    NativeGuard {
        stop,
        worker: Some(worker),
        _retention: retention,
    }
}

pub fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!(event="runtime_panic",panic=%info);
        previous(info);
    }));
}

pub fn cleanup(directory: &Path, retention: Duration) -> io::Result<()> {
    for item in fs::read_dir(directory)? {
        let item = item?;
        let name = item.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("vllm-router.")
            && name.ends_with(".jsonl")
            && item.file_type()?.is_file()
        {
            if SystemTime::now()
                .duration_since(item.metadata()?.modified()?)
                .unwrap_or_default()
                > retention
            {
                fs::remove_file(item.path())?;
            }
        }
    }
    Ok(())
}

pub struct RollingWriter {
    directory: PathBuf,
    file: File,
    day: chrono::NaiveDate,
    size: u64,
    secrets: Vec<String>,
}
impl RollingWriter {
    pub fn new(root: &Path) -> io::Result<Self> {
        let c = config();
        let directory = root.join(&c.node).join(&c.role);
        fs::create_dir_all(&directory)?;
        cleanup(&directory, c.retention)?;
        let file = Self::open(&directory)?;
        Ok(Self {
            directory,
            file,
            day: Utc::now().date_naive(),
            size: 0,
            secrets: ["OPENAI_API_KEY", "VLLM_API_KEY", "HF_TOKEN"]
                .iter()
                .filter_map(|k| std::env::var(k).ok())
                .filter(|s| !s.is_empty())
                .collect(),
        })
    }
    fn open(directory: &Path) -> io::Result<File> {
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(directory.join(format!(
                "vllm-router.{}.{}.{}.jsonl",
                Utc::now().format("%Y%m%dT%H%M%S%.6fZ"),
                std::process::id(),
                uuid::Uuid::new_v4().simple()
            )))
    }
    pub fn directory(&self) -> PathBuf {
        self.directory.clone()
    }
}
impl Write for RollingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut text = String::from_utf8_lossy(bytes).into_owned();
        for secret in &self.secrets {
            text = text.replace(secret, "<redacted>");
        }
        if self.day != Utc::now().date_naive()
            || (self.size > 0 && self.size + text.len() as u64 > config().max_bytes)
        {
            self.file.flush()?;
            self.file = Self::open(&self.directory)?;
            self.day = Utc::now().date_naive();
            self.size = 0;
        }
        if let Err(error) = self.file.write_all(text.as_bytes()) {
            metrics::counter!("vllm_router_observability_log_io_errors_total").increment(1);
            notify(
                "log_io_failed",
                "critical",
                json!({"error_kind":format!("{:?}",error.kind())}),
                "firing",
            );
            return Err(error);
        }
        self.size += text.len() as u64;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[derive(Default)]
struct Fields(serde_json::Map<String, Value>);
impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), json!(format!("{value:?}")));
    }
}
pub struct FailureLayer;
impl<S: tracing::Subscriber> Layer<S> for FailureLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _: LayerContext<'_, S>) {
        if !config().enabled || *event.metadata().level() != tracing::Level::ERROR {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let text = serde_json::to_string(&fields.0)
            .unwrap_or_default()
            .to_lowercase();
        let kind = if text.contains("out of memory") {
            "cuda_oom"
        } else if text.contains("illegal memory access") {
            "gpu_fault"
        } else if text.contains("panic") {
            "runtime_fatal"
        } else {
            return;
        };
        notify(
            kind,
            "critical",
            json!({"target":event.metadata().target(),"message":text.chars().take(2000).collect::<String>()}),
            "firing",
        );
    }
}

struct RequestState {
    id: String,
    client_id: String,
    route: String,
    start: Instant,
    status: u16,
    streaming: bool,
    first: Option<Instant>,
    last: Option<Instant>,
    max_gap: f64,
    chunks: u64,
    pending: Vec<u8>,
    done: bool,
    protocol_error: bool,
    finished: bool,
    tripped: HashSet<String>,
    phase: String,
}
impl RequestState {
    fn fields(&self) -> Value {
        json!({"request_id":self.id,"client_request_id":self.client_id,"route":self.route,"stage":self.phase,"status":self.status})
    }
    fn trip(&mut self, kind: &str, elapsed: f64, threshold: f64) {
        if self.tripped.insert(kind.into()) {
            tracing::warn!(component="router",service=%config().service,role=%config().role,node=%config().node,event=kind,request_id=%self.id,client_request_id=%self.client_id,route=%self.route,stage=%self.phase,elapsed_seconds=elapsed,threshold_seconds=threshold);
            let mut conditions = CONDITIONS.get_or_init(Mutex::default).lock().unwrap();
            let active = conditions.entry(kind.into()).or_default();
            let first = active.is_empty();
            active.insert(self.id.clone());
            drop(conditions);
            if first {
                let mut fields = self.fields();
                fields["elapsed_seconds"] = json!(elapsed);
                fields["threshold_seconds"] = json!(threshold);
                notify(kind, "warning", fields, "firing");
            }
        }
    }
    fn inspect(&mut self, bytes: &[u8]) {
        if !self.streaming {
            return;
        }
        self.pending.extend_from_slice(bytes);
        while let Some(end) = self.pending.iter().position(|b| *b == b'\n') {
            let line: Vec<_> = self.pending.drain(..=end).collect();
            let Some(data) = line.strip_prefix(b"data:") else {
                continue;
            };
            let data = String::from_utf8_lossy(data);
            let data = data.trim();
            if data == "[DONE]" {
                self.done = true;
                continue;
            }
            let Ok(event) = serde_json::from_str::<Value>(data) else {
                self.protocol_error = true;
                continue;
            };
            let kind = event["type"].as_str().unwrap_or("");
            if ["response.completed", "response.incomplete", "message_stop"].contains(&kind) {
                self.done = true;
            }
            if event.get("error").is_some_and(|v| !v.is_null())
                || kind == "response.failed"
                || kind == "error"
            {
                self.protocol_error = true;
                notify("stream_error_event", "critical", self.fields(), "firing");
            }
            let visible = event["choices"].as_array().is_some_and(|choices| {
                choices.iter().any(|c| {
                    [
                        &c["delta"]["content"],
                        &c["delta"]["reasoning_content"],
                        &c["text"],
                    ]
                    .iter()
                    .any(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                        || c["delta"]["tool_calls"]
                            .as_array()
                            .is_some_and(|a| !a.is_empty())
                })
            }) || event["delta"].as_str().is_some_and(|s| !s.is_empty())
                || ["text", "thinking", "partial_json"]
                    .iter()
                    .any(|key| event["delta"][key].as_str().is_some_and(|s| !s.is_empty()));
            if visible {
                let now = Instant::now();
                if self.first.is_none() {
                    self.first = Some(now);
                    let ttft = self.start.elapsed().as_secs_f64();
                    metrics::histogram!("vllm_router_first_output_seconds","route"=>self.route.clone()).record(ttft);
                    native_info!(event="first_output",request_id=%self.id,client_request_id=%self.client_id,route=%self.route,ttft_seconds=ttft);
                }
                if let Some(last) = self.last {
                    let gap = now.duration_since(last).as_secs_f64();
                    self.max_gap = self.max_gap.max(gap);
                    metrics::histogram!("vllm_router_output_chunk_gap_seconds","route"=>self.route.clone()).record(gap);
                }
                self.last = Some(now);
                self.chunks += 1;
            }
        }
        if self.pending.len() > 1024 * 1024 {
            self.pending.clear();
            self.protocol_error = true;
        }
    }
    fn finish(&mut self, forced: Option<&str>) {
        if self.finished {
            return;
        }
        self.finished = true;
        let elapsed = self.start.elapsed().as_secs_f64();
        if elapsed > config().e2e {
            self.trip("request_e2e_slow", elapsed, config().e2e);
        }
        let outcome = forced.unwrap_or(
            if self.protocol_error || self.status >= 500 || self.streaming && !self.done {
                "error"
            } else if self.status >= 400 {
                "client_error"
            } else {
                "success"
            },
        );
        if outcome == "error" {
            notify("request_failed", "critical", self.fields(), "firing");
        }
        COMPLETED.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("vllm_router_response_completed_total","route"=>self.route.clone(),"outcome"=>outcome.to_owned()).increment(1);
        metrics::histogram!("vllm_router_response_complete_seconds","route"=>self.route.clone())
            .record(elapsed);
        let terminal_reason = if forced == Some("cancelled") {
            "cancelled"
        } else if forced.is_some() {
            "response_body_error"
        } else if self.status >= 500 {
            "http_5xx"
        } else if self.status >= 400 {
            "http_4xx"
        } else if self.protocol_error {
            "stream_protocol_error"
        } else if self.streaming && !self.done {
            "stream_truncated"
        } else if self.streaming {
            "stream_completed"
        } else {
            "response_completed"
        };
        native_info!(event="request_finished",streaming=self.streaming,terminal_reason,request_id=%self.id,client_request_id=%self.client_id,route=%self.route,status=self.status,outcome,
            e2e_seconds=elapsed,ttft_seconds=self.first.map(|t|t.duration_since(self.start).as_secs_f64()).unwrap_or(f64::NAN),output_chunks=self.chunks,max_chunk_gap_seconds=self.max_gap,protocol_done=self.done);
        let mut resolved = Vec::new();
        {
            let mut conditions = CONDITIONS.get_or_init(Mutex::default).lock().unwrap();
            for kind in &self.tripped {
                if let Some(active) = conditions.get_mut(kind) {
                    active.remove(&self.id);
                    if active.is_empty() {
                        resolved.push(kind.clone());
                    }
                }
            }
        }
        for kind in resolved {
            let mut fields = self.fields();
            fields["outcome"] = json!(outcome);
            notify(&kind, "warning", fields, "resolved");
        }
        requests().remove(&self.id);
    }
}

fn check_inflight() {
    // Clone values before locking: completion removes entries without a shard-lock inversion.
    let active: Vec<_> = requests()
        .iter()
        .map(|entry| entry.value().clone())
        .collect();
    for state in active {
        let mut s = state.lock().unwrap();
        if s.finished {
            continue;
        }
        let elapsed = s.start.elapsed().as_secs_f64();
        if elapsed > config().e2e {
            s.trip("request_e2e_slow", elapsed, config().e2e);
        }
    }
}

pub async fn request_middleware(mut request: Request, next: Next) -> Response {
    let path = request.uri().path().to_owned();
    if !config().enabled
        || ![
            "/v1/chat/completions",
            "/v1/completions",
            "/v1/responses",
            "/v1/messages",
            "/generate",
        ]
        .contains(&path.as_str())
    {
        return next.run(request).await;
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    let client_id = request
        .extensions()
        .get::<crate::middleware::RequestId>()
        .map(|id| id.0.clone())
        .or_else(|| {
            request
                .headers()
                .get("x-request-id")
                .and_then(|s| s.to_str().ok())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| id.clone());
    request
        .headers_mut()
        .insert(OBS_ID, HeaderValue::from_str(&id).unwrap());
    if !request.headers().contains_key("x-request-id") {
        if let Ok(value) = HeaderValue::from_str(&client_id) {
            request.headers_mut().insert("x-request-id", value);
        }
    }
    let state = Arc::new(Mutex::new(RequestState {
        id: id.clone(),
        client_id: client_id.clone(),
        route: path.clone(),
        start: Instant::now(),
        status: 0,
        streaming: false,
        first: None,
        last: None,
        max_gap: 0.,
        chunks: 0,
        pending: Vec::new(),
        done: false,
        protocol_error: false,
        finished: false,
        tripped: HashSet::new(),
        phase: "router".into(),
    }));
    requests().insert(id.clone(), state.clone());
    STARTED.fetch_add(1, Ordering::Relaxed);
    let mut guard = PendingRequest(Some(state.clone()));
    native_info!(event="request_started",request_id=%id,client_request_id=%client_id,route=%path);
    let response = next.run(request).await;
    {
        let mut s = state.lock().unwrap();
        s.status = response.status().as_u16();
        s.streaming = response
            .headers()
            .get("content-type")
            .is_some_and(|v| v.as_bytes().starts_with(b"text/event-stream"));
        native_info!(event="response_headers",request_id=%id,client_request_id=%client_id,status=s.status,elapsed_seconds=s.start.elapsed().as_secs_f64());
    }
    let (mut parts, body) = response.into_parts();
    parts
        .headers
        .insert(OBS_ID, HeaderValue::from_str(&id).unwrap());
    guard.0.take();
    Response::from_parts(
        parts,
        Body::from_stream(ObservedStream {
            inner: Box::pin(body.into_data_stream()),
            state,
        }),
    )
}

struct PendingRequest(Option<Arc<Mutex<RequestState>>>);
impl Drop for PendingRequest {
    fn drop(&mut self) {
        if let Some(state) = self.0.take() {
            state.lock().unwrap().finish(Some("cancelled"));
        }
    }
}

struct ObservedStream {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, axum::Error>> + Send>>,
    state: Arc<Mutex<RequestState>>,
}
impl Stream for ObservedStream {
    type Item = Result<Bytes, axum::Error>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(bytes))) => {
                self.state.lock().unwrap().inspect(&bytes);
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.state.lock().unwrap().finish(Some("error"));
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.state.lock().unwrap().finish(None);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl Drop for ObservedStream {
    fn drop(&mut self) {
        self.state.lock().unwrap().finish(Some("cancelled"));
    }
}

pub struct ClientObservation {
    state: Option<Arc<Mutex<RequestState>>>,
    start: Instant,
    phase: String,
    url: String,
    backend_id: String,
}
pub fn client_request(
    mut builder: reqwest::RequestBuilder,
    headers: Option<&HeaderMap>,
    phase: Option<&str>,
    url: &str,
) -> (reqwest::RequestBuilder, ClientObservation) {
    if !config().enabled {
        return (
            builder,
            ClientObservation {
                state: None,
                start: Instant::now(),
                phase: String::new(),
                url: String::new(),
                backend_id: String::new(),
            },
        );
    }
    let state = headers
        .and_then(|h| h.get(OBS_ID))
        .and_then(|v| v.to_str().ok())
        .and_then(|id| requests().get(id).map(|s| s.value().clone()));
    let backend = builder
        .try_clone()
        .and_then(|r| r.build().ok())
        .and_then(|r| {
            r.headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        })
        .unwrap_or_default();
    let mut observation = ClientObservation {
        state: state.clone(),
        start: Instant::now(),
        phase: phase.unwrap_or("backend").into(),
        url: url.split('?').next().unwrap_or(url).into(),
        backend_id: backend,
    };
    if let Some(state) = state {
        let mut s = state.lock().unwrap();
        s.phase = observation.phase.clone();
        if observation.backend_id.is_empty() {
            observation.backend_id = s.client_id.clone();
            builder = builder.header("X-Request-Id", &s.client_id);
        }
        builder = builder
            .header("X-Observability-Phase", &observation.phase)
            .header("X-Client-Request-Id", &s.client_id)
            .header("X-Observability-Request-Id", &s.id);
        native_info!(event="backend_request_started",request_id=%s.id,client_request_id=%s.client_id,backend_request_id=%observation.backend_id,phase=%observation.phase,backend_url=%observation.url);
    }
    (builder, observation)
}
impl ClientObservation {
    pub fn finish(&self, status: Option<u16>, failed: bool) {
        if let Some(state) = &self.state {
            let s = state.lock().unwrap();
            let duration = self.start.elapsed().as_secs_f64();
            native_info!(event="backend_response_headers",request_id=%s.id,client_request_id=%s.client_id,backend_request_id=%self.backend_id,
                phase=%self.phase,backend_url=%self.url,status=status.unwrap_or(0),transport_error=failed,headers_seconds=duration);
            metrics::histogram!("vllm_router_backend_headers_seconds","phase"=>self.phase.clone())
                .record(duration);
            if failed || status.is_some_and(|status| status >= 500) {
                notify(
                    "backend_request_failed",
                    "critical",
                    json!({"client_request_id":s.client_id,"backend_request_id":self.backend_id,"phase":self.phase,"status":status}),
                    "firing",
                );
            }
        }
    }
}
