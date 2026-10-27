use serde_json::{json, Map, Value};
use std::path::PathBuf;
use tracing::field::{Field, Visit};
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::time::ChronoUtc;
use tracing_subscriber::fmt::{
    format::Writer, FmtContext, FormatEvent, FormatFields, FormattedFields,
};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use crate::config::TraceConfig;

#[derive(Default)]
struct JsonFields(Map<String, Value>);
impl Visit for JsonFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), json!(format!("{value:?}")));
    }
}

/// Capture once before queueing; derive both representations from the same instant.
struct UnifiedJsonFormat;
impl<S, N> FormatEvent<S, N> for UnifiedJsonFormat
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let now = chrono::Utc::now();
        let meta = event.metadata();
        let mut fields = JsonFields::default();
        event.record(&mut fields);
        fields.0.remove("observation_id");
        fields.0.insert(
            "timestamp".into(),
            json!(now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        );
        fields.0.insert(
            "timestamp_unix_ns".into(),
            json!(now
                .timestamp_nanos_opt()
                .expect("timestamp in Unix nanosecond range")),
        );
        fields
            .0
            .insert("level".into(), json!(meta.level().as_str()));
        fields.0.insert("target".into(), json!(meta.target()));
        if let Some(file) = meta.file() {
            fields.0.insert("filename".into(), json!(file));
        }
        if let Some(line) = meta.line() {
            fields.0.insert("line_number".into(), json!(line));
        }
        if let Some(scope) = ctx.event_scope() {
            let spans: Vec<Value> = scope
                .from_root()
                .map(|span| {
                    let extensions = span.extensions();
                    let mut value = extensions
                        .get::<FormattedFields<N>>()
                        .and_then(|f| serde_json::from_str::<Map<String, Value>>(&f.fields).ok())
                        .unwrap_or_default();
                    value.insert("name".into(), json!(span.name()));
                    Value::Object(value)
                })
                .collect();
            if let Some(last) = spans.last() {
                fields.0.insert("span".into(), last.clone());
            }
            fields.0.insert("spans".into(), json!(spans));
        }
        writeln!(writer, "{}", Value::Object(fields.0))
    }
}

/// Configuration for the logging system
#[derive(Debug, Clone)]
pub struct LoggingConfig {
    /// Log level for the application (default: INFO)
    pub level: Level,
    /// Whether to use json format for logs (default: false)
    pub json_format: bool,
    /// Path to store log files. If None, logs will only go to stdout/stderr
    pub log_dir: Option<String>,
    /// Whether to colorize logs when output is a terminal (default: true)
    pub colorize: bool,
    /// Log file name to use if log_dir is specified (default: "vllm-router")
    pub log_file_name: String,
    /// Custom log targets to filter (default: "vllm_router_rs")
    pub log_targets: Option<Vec<String>>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: Level::INFO,
            json_format: false,
            log_dir: None,
            colorize: true,
            log_file_name: "vllm-router".to_string(),
            log_targets: Some(vec!["vllm_router_rs".to_string()]),
        }
    }
}

/// Guard that keeps the file appender worker thread alive
///
/// This must be kept in scope for the duration of the program
/// to ensure logs are properly written to files
#[allow(dead_code)]
pub struct LogGuard {
    _native_guard: Option<crate::observability::NativeGuard>,
    _file_guard: Option<WorkerGuard>,
}

/// Initialize the logging system with the given configuration
///
/// # Arguments
/// * `config` - Configuration for the logging system
///
/// # Returns
/// A LogGuard that must be kept alive for the duration of the program
///
/// # Panics
/// Will not panic, as initialization errors are handled gracefully
pub fn init_logging(mut config: LoggingConfig, otel_layer_config: Option<TraceConfig>) -> LogGuard {
    if config.log_dir.is_none() {
        config.log_dir = std::env::var("OBSERVABILITY_LOG_DIR")
            .ok()
            .filter(|p| !p.is_empty())
            .map(|root| {
                PathBuf::from(root)
                    .join("router")
                    .to_string_lossy()
                    .into_owned()
            });
    }
    // Note: Do NOT call LogTracer::init() here. The tracing_subscriber
    // try_init() method handles LogTracer initialization internally (when the
    // "tracing-log" feature is enabled). Calling it beforehand causes
    // try_init() to fail because log::set_logger() can only be called once.

    // Convert log level to filter string
    let level_filter = match config.level {
        Level::TRACE => "trace",
        Level::DEBUG => "debug",
        Level::INFO => "info",
        Level::WARN => "warn",
        Level::ERROR => "error",
    };

    // Create env filter
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // Format: <target>=<level>,<target2>=<level2>,...
        let filter_string = if let Some(targets) = &config.log_targets {
            targets
                .iter()
                .enumerate()
                .map(|(i, target)| {
                    if i > 0 {
                        format!(",{}={}", target, level_filter)
                    } else {
                        format!("{}={}", target, level_filter)
                    }
                })
                .collect::<String>()
        } else {
            format!("vllm_router_rs={}", level_filter)
        };

        EnvFilter::new(filter_string)
    });

    // When OTel tracing is enabled, ensure the OTel span target passes through
    // the global EnvFilter regardless of log level. Without this, --log-level warn
    // silently suppresses all info-level OTel spans.
    let env_filter = if std::env::var("OBSERVABILITY_ENABLED").as_deref() != Ok("0")
        && config.log_dir.is_some()
    {
        env_filter.add_directive(
            "vllm_router_rs::observability=info"
                .parse()
                .expect("valid observability filter"),
        )
    } else {
        env_filter
    };
    let env_filter = if otel_layer_config.is_some() {
        env_filter.add_directive(
            "otel_trace=trace"
                .parse()
                .expect("valid EnvFilter directive"),
        )
    } else {
        env_filter
    };

    // Setup stdout/stderr layer
    let mut layers = Vec::new();

    // Standard timestamp format: YYYY-MM-DD HH:MM:SS
    let time_format = "%Y-%m-%dT%H:%M:%S%.3fZ".to_string();
    let json_format = config.log_dir.is_some()
        || config.json_format
        || std::env::var("OBSERVABILITY_LOG_FORMAT").as_deref() == Ok("json");

    // Configure the console stdout layer
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_ansi(config.colorize)
        .with_file(true)
        .with_line_number(true)
        .with_timer(ChronoUtc::new(time_format.clone()));

    let stdout_layer = if json_format {
        stdout_layer.json().event_format(UnifiedJsonFormat).boxed()
    } else {
        stdout_layer.boxed()
    };

    // With file logging configured, allow skipping duplicate console formatting.
    // Keep stdout as a fallback when no file destination is configured.
    let console_enabled =
        config.log_dir.is_none() || std::env::var("OBSERVABILITY_CONSOLE").as_deref() == Ok("1");
    if console_enabled {
        layers.push(stdout_layer);
    }

    // Create a file appender if log_dir is specified
    let mut file_guard = None;
    let mut native_directory = None;
    let mut dropped = None;

    if let Some(log_dir) = &config.log_dir {
        let file_name = config.log_file_name.clone();
        let log_dir = PathBuf::from(log_dir);

        // Create log directory if it doesn't exist
        if !log_dir.exists() {
            if let Err(e) = std::fs::create_dir_all(&log_dir) {
                eprintln!("Failed to create log directory: {}", e);
                panic!("Cannot start with an unwritable requested log directory");
            }
        }

        let _ = file_name; // Native filenames include timestamp, PID and a unique suffix.
        let file_appender = crate::observability::RollingWriter::new(&log_dir)
            .expect("cannot initialize native rolling log files");
        native_directory = Some(file_appender.directory());

        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        dropped = Some(non_blocking.error_counter());
        file_guard = Some(guard);

        let file_layer = tracing_subscriber::fmt::layer()
            .with_ansi(false) // Never use ANSI colors in log files
            .with_file(true)
            .with_line_number(true)
            .with_timer(ChronoUtc::new(time_format))
            .with_writer(non_blocking);

        let file_layer = if json_format {
            file_layer.json().event_format(UnifiedJsonFormat).boxed()
        } else {
            file_layer.boxed()
        };

        layers.push(file_layer);
    }

    let mut prepared_otel = None;
    if let Some(trace_config) = otel_layer_config {
        match crate::otel_trace::prepare_otel(&trace_config) {
            Ok(prepared) => {
                layers.push(prepared.layer());
                prepared_otel = Some(prepared);
            }
            Err(e) => {
                eprintln!("Failed to initialize OpenTelemetry: {e}");
            }
        }
    }

    // Initialize the subscriber with all layers
    // Use try_init to handle errors gracefully in case another subscriber is already set
    let native_guard = crate::observability::initialize(native_directory, dropped);
    let subscriber_installed = tracing_subscriber::registry()
        .with(env_filter)
        .with(crate::observability::FailureLayer)
        .with(layers)
        .try_init()
        .is_ok();

    // Only activate OTel (set global propagator + provider, flip ENABLED)
    // after both the layer and subscriber are live
    if let Some(prepared) = prepared_otel {
        if subscriber_installed {
            crate::otel_trace::activate_otel(prepared);
        }
    }

    // Return the guard to keep the file appender worker thread alive
    crate::observability::install_panic_hook();
    LogGuard {
        _native_guard: Some(native_guard),
        _file_guard: file_guard,
    }
}
