# Request lifecycle JSONL and essential alerts

✅ Disabled by default. Set these variables on Router, Prefill and Decode services, then restart:

```bash
export OBSERVABILITY_ENABLED=1
export OBSERVABILITY_LOG_DIR=/path/to/shared/inference-logs/instance-id
```

✅ Enabling requires a writable absolute directory. Missing directories are created. The deployer
chooses the service instance directory and shared mount. Model and routing settings stay
unchanged. Lifecycle JSONL bypasses native log levels. Native stdout/stderr, native file logging
and redirection remain deployment-owned; no native output collector or Mooncake wrapper is
installed. Router `--log-dir` controls native file logs independently of lifecycle JSONL.

## Files and optional roles

✅ The default layout needs no role configuration:

```text
<root>/vllm/<hostname>/<UTC>.<PID>.jsonl
<root>/router/<hostname>/<UTC>.<PID>.jsonl
```

✅ UTC uses `YYYYMMDDTHHMMSS.ffffffZ` , e.g. `20261103T032459.831234Z.12345.jsonl` . It is the
file creation time, not the request start. No component prefix or UUID is added to filenames.
Creation is exclusive: an existing file, symlink or directory is never overwritten. Collisions
retry `.1.jsonl` , `.2.jsonl` , etc., with a maximum of 10,000 candidates including the
unsuffixed name. Other errors propagate immediately; exhaustion follows the normal I/O failure
path.

✅ The `role` field defaults to `vllm` or `router` . `OBSERVABILITY_ROLE` is optional: only a
value that differs from the component name adds a role subdirectory, e.g.
`<root>/vllm/<host>/p0/` . Default or explicitly repeated component roles do not add
directories. Roles `.` and `..` are invalid. Use the existing `phase` , process and engine
fields when no custom role is needed. The vLLM hostname comes from `socket.gethostname()` ;
Router uses `HOSTNAME` or the kernel hostname. In containers these identify the container's
visible hostname, not necessarily the physical host.

| Setting | Default / behavior |
| --- | --- |
| `OBSERVABILITY_LOG_MAX_MB` | ✅ 256 MiB; also rotate on UTC date changes |
| `OBSERVABILITY_LOG_RETENTION_DAYS` | ✅ 0; manual cleanup, positive days enable mtime-based cleanup |
| `OBSERVABILITY_ROLE` | ✅ Component name; only custom values add a directory |
| `OBSERVABILITY_ALERT_CONFIG` | ✅ Unset; private alert configuration, no external delivery by default |

✅ Cleanup matches only the generated filename schemas, including the old component/UTC/PID/UUID
names. The old default `<host>/vllm/` or `<host>/router/` directory is also cleaned when using
the default layout. Other custom role directories, unrelated files and symlinks are preserved.
Existing files are not renamed or moved. Readers should scan the component subtree for both
naming schemes and optional role directories. Retention does not limit total disk usage.

✅ A bounded asynchronous writer buffers records; queue overflow can drop logs. I/O failures are
reported through counters and essential alerts. Writes are not individually fsynced, and forced
termination may lose the buffered tail. There are no per-token or per-kernel log records.

## Lifecycle and timing

✅ A canonical `request_id` correlates Router entry/dispatch, vLLM API, engine, scheduler,
worker, NIXL pull, decode and completion. Internal connector IDs are preserved. Clients receive
`X-Observability-Request-Id` . Stages depend on the actual branch; cache hits, recomputation,
cancellation and errors need not have every event. NIXL push and Mooncake internals are not
individually instrumented.

✅ `timestamp` is UTC with millisecond display precision. `timestamp_unix_ns` comes from the same
event time; nanosecond units do not promise nanosecond accuracy. Durations use process monotonic
clocks. Cross-host ordering requires clock synchronization. `first_output` includes nonempty
visible reasoning, content or tool calls at the server; this is not client network arrival time.
No generated text is written to lifecycle logs.

## Alerts

✅ For optional Feishu or generic webhook alerts, set `OBSERVABILITY_ALERT_CONFIG` to a private
file:

```json
{
  "url": "https://open.feishu.cn/open-apis/bot/v2/hook/REPLACE_ME",
  "secret": "optional-bot-signing-secret",
  "slow_request_seconds": 30
}
```

✅ Omit `secret` for unsigned bots. Generic webhooks can set `"format": "webhook"` in this file.
Feishu delivery checks business codes and retries up to three times. GPU/KV faults, corrupted
output, request failures, overall latency and log failures are covered; Router owns request
failure and end-to-end latency alerts. The default overall threshold is 30 seconds, even during
visible reasoning. This threshold triggers alerts; it does not cancel requests. Equivalent
alerts are rate limited for five minutes per process by default;
`OBSERVABILITY_ALERT_COOLDOWN_S` changes that positive interval. No disk replay or inference probe is
added. Infrastructure monitoring remains responsible for process and node termination.

## Reasoning summary

✅ `request_finished` includes request-wide observations without fragment logs:

| Field | Meaning |
| --- | --- |
| `reasoning_first_seconds` | ✅ Request start to first nonempty visible reasoning fragment |
| `reasoning_last_seconds` | ✅ Request start to last nonempty reasoning delta |
| `content_first_seconds` | ✅ Request start to first nonempty visible content fragment |
| `reasoning_to_content_seconds` | ✅ First reasoning to first content; null if content came first |
| `reasoning_tokens` | ✅ Final protocol-provided reasoning count, or null (format below) |
| `reasoning_tokens_complete` | ✅ True only when a successful response supplies the final count |

All durations use the process monotonic clock and seconds. Unobserved times are JSON null. Times
aggregate all choices; token counts use the final aggregate rather than summing chunks. For Chat
Completions / Completions SSE, the authoritative usage chunk has `choices: []` ; usage accompanying
choice updates is intermediate. Responses reads
`response.usage.output_tokens_details.reasoning_tokens` from terminal `response.completed` /
`response.incomplete` events, or top-level usage in nonstream Responses JSON. Messages usage does
not provide a separate reasoning count, so it remains null with completeness false; total output
tokens are never substituted. Missing final usage stays unknown, including reasoning-only streams
without usage. Zero is a known count. Failed, truncated, or cancelled responses preserve observed
times but do not claim complete token counts. No usage option is forced and no text is retokenized
or retained in logs. Nonstream responses only supply usage, with null stage times. Nonstream JSON
observation is bounded to 1 MiB; larger bodies have unknown usage in this summary. Hidden reasoning
has null reasoning times. Tool calls count toward `first_output` but do not set content time.
Responses records reasoning-text / reasoning-summary-text and output-text deltas. Messages records
thinking / text block starts and deltas. Tool starts and argument fragments count toward first
output without setting content time; redacted thinking does not imply a reasoning time. These
observations cover inference POST endpoints. Responses retrieval/list/delete/cancel control requests
do not create a separate generation summary; response-stream cancellation is recorded.

A client that closes its transport after the complete SSE terminal event is recorded as a
completed stream, preserving final usage. Closing before that event remains cancellation;
protocol and response-body errors remain errors. This only affects observation, not forwarded
response bytes. Periodic `observability_status` records include `log_dropped`, `log_io_errors`,
`webhook_errors`, and `alert_dropped`, including explicit zero values for healthy counters.

## Runtime boundaries

SSE byte assembly and SSE JSON parsing run outside the shared request mutex. The stream
keeps at most 1 MiB for an incomplete line, discards an oversized line through its
newline, and continues observing later records. Response bytes remain unchanged.
A brief per-request lock applies parsed observations; poisoned locks recover and
increment `vllm_router_observability_mutex_poison_total`. Parsing remains synchronous
CPU work in the async stream; this does not promise zero executor cost.

The inflight registry holds weak references. The watchdog removes dead entries and
uses `try_lock` to skip busy requests, checking them on a later pass.
`OBSERVABILITY_INFLIGHT_SCAN_S` sets the positive scan interval (default 200 milliseconds).
Active requests are never expired solely by age. The slow threshold is an absolute
end-to-end alert, including visible reasoning; raise `slow_request_seconds` to match
long-generation workloads. No token-based duration is inferred. Alert grouping uses
a fixed set of alert kinds and process labels, not request IDs. The start-time map
is populated only by firing events and cleared on resolved events.

The caller's request ID is JSON-escaped and is separate from the router-generated
canonical correlation ID. Original native console rendering is deployment-owned.
The project currently uses Rust edition 2021; environment-mutating integration
tests require a separate migration audit before an edition-2024 upgrade.
