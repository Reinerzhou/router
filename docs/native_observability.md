# Native request observability

Add these variables to the existing Router service and restart:

```bash
export OBSERVABILITY_ENABLED=1
export OBSERVABILITY_LOG_DIR=/path/to/shared/inference-logs
```

Use the same settings on the companion vLLM services. No model, topology, or
routing flags need to change. Router JSONL files are created under
`<root>/router/<hostname>/<role>/`; an existing explicit `--log-dir` takes
precedence. `OBSERVABILITY_ROLE` defaults to `router` and should be set separately
for colocated vLLM processes.

Files rotate daily in UTC and at `OBSERVABILITY_LOG_MAX_MB` (default 256 MiB).
Filenames contain the UTC date; a busy day can have multiple segments.
`OBSERVABILITY_LOG_RETENTION_DAYS` defaults to `0`: no automatic deletion, so
operators can manually remove files older than seven days according to space
needs. Set it to `7` to enable automatic cleanup. These limits apply to segments
and retention, not the total directory size. The bounded
background writer reports dropped records and I/O errors. File logging defaults
to JSON without duplicate console formatting.

The Router assigns a canonical `request_id`, preserves the client and connector
IDs, forwards correlation headers to P/D, and returns
`X-Observability-Request-Id`. Events include entry, backend dispatch and headers,
first useful output, and full response completion. Cancellation, truncated SSE,
protocol errors, and HTTP failures remain distinct. Empty role-only chunks do
not count as first output. Structured framework and request records both include
UTC `timestamp` and integer `timestamp_unix_ns` from a single clock reading.
Durations use `Instant`; cross-host wall-clock ordering requires clock sync.

Optional Feishu alerts use a private JSON file:

```json
{
  "url": "https://open.feishu.cn/open-apis/bot/v2/hook/REPLACE_ME",
  "secret": "optional-bot-signing-secret",
  "slow_request_seconds": 30
}
```

Set `OBSERVABILITY_ALERT_CONFIG` to its path. Omit the secret for unsigned bots.
Messages contain the keyword `推理告警`. Delivery supports signed text messages,
checks HTTP and Feishu business codes, and retries up to three times. Only
request failures, end-to-end latency, fatal faults, and logging failures notify;
HTTP 4xx and client cancellation do not. Equivalent notifications are rate
limited for five minutes per process; latency recovery is emitted when the
active condition clears. There is no delivery replay queue or inference probe.
Infrastructure monitoring must cover process/node termination.

Set `OBSERVABILITY_ENABLED=0` and restart to disable added request observations.
Basic file logs can remain enabled through the existing logging configuration.

The CPU fixture can be run against a built Router executable:

```bash
python3 tests/native_observability_integration.py \
  --router-binary target/release/vllm-router --output-dir /tmp/router-observability
```
