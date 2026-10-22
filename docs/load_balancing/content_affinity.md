# HTTP content affinity

## Goal and scope

The regular HTTP router can keep repeated conversation prefixes near the worker that previously received them. It derives a routing key from the request body, so clients do not need to send a session ID. The key affects worker selection only; the original JSON and media are sent to the backend.

This applies to `/v1/chat/completions`, `/v1/responses`, and `/v1/messages` when the selected policy is `cache_aware`. Chat Completions uses the typed request path. Responses and Messages use the transparent proxy path and the default policy. A transparent deployment should give this policy replicas of the same model; it does not filter workers by the body's `model` field.

## From request to worker

1. Build a stable key from prompt fields. Preserve order and message boundaries. Ignore session IDs, request IDs, sampling controls and request metadata.
2. Compare that key with the approximate radix-tree paths recorded for each healthy worker. A prefix counts for a worker only if that worker owns the matched path. The best match must exceed `cache_threshold`.
3. On a usable hit, choose the least busy worker among equal best holders. Spill to the least busy healthy worker only when the chosen holder exceeds **both** the absolute and relative load margins above the healthy-worker mean.
4. On a miss, choose the least busy healthy worker. Record the final placement for later requests. In-flight request counts supply the load estimate, and periodic eviction bounds the remembered tree size per worker.

The tree records routing decisions, not actual engine KV ownership. A matched key means a chance to reuse a prefix. Eviction, failed requests, differing chat templates, hidden server state and context truncation can all make that estimate stale. This is the text-prefix part of the SMG-inspired approach; no engine KV-event feed is required or implemented here.

## Key content by endpoint

| Endpoint | Included in the key | Deliberately excluded |
|---|---|---|
| Chat Completions | Ordered tool schemas; ordered message roles, names, reasoning, content, tool calls and results | `session_params.session_id`, sampling options, metadata |
| Responses | `instructions`; ordered tool schemas; explicit `input` text, messages, function calls/results and other visible input items | `previous_response_id`, sampling options, metadata |
| Messages | `system`; ordered tool schemas; ordered messages and nested content blocks | Sampling options, metadata |

Strings are JSON escaped without trimming. Message and array boundaries are explicit; object keys are processed in a stable order. Tool order is retained because a backend template may render different orders differently. Inline data URLs and base64 image payloads are represented by short hashes in the key; URLs and file IDs remain visible references. The backend still receives the original media bytes.

Responses requests that rely on `previous_response_id` or `item_reference` can refer to history absent from the current request. The router does not bind response IDs to workers, so affinity for backend-held conversation state is not guaranteed. For stateless content affinity, send the relevant history explicitly, commonly with `store=false`. A request containing only `previous_response_id` has an empty content key and follows the miss path.

## HTTP and request lifetime

The typed and transparent paths forward end-to-end API headers such as `anthropic-version`, `anthropic-beta` and `x-api-key`. They remove hop-by-hop headers, headers named by `Connection`, and incoming JSON framing headers. Trace headers use the existing OpenTelemetry propagation path. A configured backend key replaces caller `Authorization`; otherwise caller authorization passes through. When the router selects a data-parallel rank, its rank header replaces the incoming one; without a selected rank, the caller's rank header passes through.

With `cache_aware`, all three paths count an active request while upstream work is in progress. Non-streaming requests release the count after the body is read. Streaming requests release it on EOF, read error or client disconnect. The response body keeps the guard alive and forwards chunks with backpressure. The backend's content type is preserved, including JSON errors returned for streaming requests.

## Verification and measured result

The regression cases in `routing_text.rs`, `header_utils.rs`, `router.rs` and `cache_aware.rs` cover key stability and differences, nested media, header forwarding, cancellation, stream errors, concurrent load guards and hit/spill selection. These cases are source-level checks; the H200 run below exercised the assembled service.

On 2026-10-21, two independent vLLM 0.28.0 workers shared one H200 GPU and served Qwen2.5-7B-Instruct through this router. Each condition restarted the stack to clear caches. Three seeds used 16 prompt families, four rounds of full conversation history and concurrency 8. All 33 supported conditions completed: **2,112/2,112** workload requests succeeded. Direct-worker and router smoke requests succeeded for all three endpoints. Local Codex CLI through Responses and Claude CLI through Messages each completed one Router-backed request.

The measured current-router binary has SHA256 `1d9a3b26391cdb5e0a4c815bdd5262a22664355ad5c0065b47d0fad417b0d5e9`. The later DP-rank forwarding, streaming error media-type and load-metric cleanups were compile checked but were not part of that GPU run.

The table reports the mean token cache-hit ratio across three seeds and the median of each seed's p50 time to first token (TTFT, milliseconds). `A` is current `cache_aware`; `B` is current `round_robin`; `C` is the `9ffe129` cache-aware baseline.

| Endpoint | A hit | B hit | C hit | A TTFT p50 | B TTFT p50 | C TTFT p50 |
|---|---:|---:|---:|---:|---:|---:|
| Chat Completions | 72.0% | 54.0% | 51.4% | 58.3 | 154.9 | 155.7 |
| Responses | 74.1% | 56.0% | 50.6% | 56.6 | 143.0 | 109.4 |
| Messages | 72.5% | 50.5% | unsupported | 54.5 | 141.6 | — |

A fourth condition disabled vLLM prefix caching. Its warm p50 TTFT was 321.4, 338.0 and 298.5 ms for Chat, Responses and Messages, versus 53.6, 51.3 and 51.9 ms with A. This supports the cache-reuse interpretation for this workload. The Responses baseline had a lower p95 than A in this run, so these results do not establish a tail-latency improvement.

The experiment used one GPU for both workers and a fixed synthetic workload. It demonstrates request compatibility and affinity under these conditions; it does not measure multi-node throughput or production cache distributions. The local experiment record is `affinity-experiments/results/h200-20261021/REPORT.md` in the parent workspace.
