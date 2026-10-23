# HTTP content affinity

## Goal and scope

The regular HTTP router can keep repeated conversation prefixes near the worker that previously received them. It derives a routing key from the request body, so clients do not need to send a session ID. The key affects worker selection only; the original JSON and media are sent to the backend.

This applies to `/v1/chat/completions`, `/v1/responses`, and `/v1/messages` when the selected policy is `cache_aware`. Chat Completions uses the typed request path. Responses and Messages use the transparent proxy path and the default policy. A transparent deployment should give this policy replicas of the same model; it does not filter workers by the body's `model` field.

## From request to worker

1. Build a conservative key: the full prompt-rendering configuration and first input item must match before any prefix counts. Later input items are compared as complete units. Ignore session IDs, request IDs, sampling controls and request metadata.
2. Compare that key with the approximate radix-tree paths recorded for each healthy worker. A prefix counts for a worker only if that worker owns the matched path. The best match must exceed `cache_threshold`.
3. On a usable hit, choose the least busy worker among equal best holders. Spill to the least busy healthy worker only when the chosen holder exceeds **both** the absolute and relative load margins above that worker.
4. On a miss, choose the least busy healthy worker. Record the final placement for later requests. In-flight request counts supply the load estimate, and periodic eviction bounds the remembered tree size per worker.

The tree records routing decisions, not actual engine KV ownership. A matched key means a chance to reuse a prefix. Eviction, failed requests, differing chat templates, hidden server state and context truncation can all make that estimate stale. This is the text-prefix part of the SMG-inspired approach; no engine KV-event feed is required or implemented here.

## Key content by endpoint

| Endpoint | Included in the key | Deliberately excluded |
|---|---|---|
| Chat Completions | Full rendering configuration, including `tools`, `tool_choice` and `chat_template_kwargs`; ordered complete `messages` | `session_params.session_id`, sampling options, metadata |
| Responses | Full rendering configuration, including `instructions` and `tools`; ordered complete explicit `input` items | Sampling options, metadata |
| Messages | Full rendering configuration, including `system` and `tools`; ordered complete `messages` | Sampling options, metadata |

Each complete item and the rendering configuration are guarded by a hash before their diagnostic text is appended to the key. This deliberately gives up reuse within a changed item: key field order cannot safely stand in for model-specific chat-template order. Unknown request options are kept in the rendering configuration, so different values do not share an affinity key. Inline data URLs and base64 image payloads are represented by short hashes in the diagnostic text; the backend still receives the original media bytes.

For Responses, per-message `id`, `prompt_cache_key` and `client_metadata` are excluded: clients such as Codex change them between calls without changing the rendered prompt. `tool_choice` and other rendering options remain part of the configuration guard.

Responses requests using `previous_response_id`, `conversation` or an `item_reference` can refer to history absent from the current request. They receive an empty content key and follow the miss path. The router does not bind response IDs to workers; for content affinity, send the relevant history explicitly, commonly with `store=false`.

The new content key applies only to `cache_aware`. Chat requests under other policies retain their previous session-ID routing input; transparent routes under other policies retain the serialized request body.

## HTTP and request lifetime

The typed and transparent paths forward end-to-end API headers such as `anthropic-version`, `anthropic-beta` and `x-api-key`. They remove hop-by-hop headers, headers named by `Connection`, and incoming JSON framing headers. Trace headers use the existing OpenTelemetry propagation path. A configured backend key replaces caller `Authorization`; otherwise caller authorization passes through. When the router selects a data-parallel rank, its rank header replaces the incoming one; without a selected rank, the caller's rank header passes through.

With `cache_aware`, all three paths count an active request while upstream work is in progress. Non-streaming requests release the count after the body is read. Streaming requests release it on EOF, read error or client disconnect. The response body keeps the guard alive and forwards chunks with backpressure. The backend's content type is preserved, including JSON errors returned for streaming requests.

## Verification and measured result

The tested release binary has SHA256 `923c5a495d377e885f616a75bcf4dbeb737223588890c93f6b1d4622fdfaedd8`. The single H200 acceptance run uses two independent vLLM 0.28.0 workers on one GPU, Qwen2.5-7B-Instruct, two seeds, 16 prompt families, four rounds of explicit conversation history, and concurrency 8. Each condition restarts the stack. Groups A and B compare `cache_aware` with `round_robin`; group D keeps `cache_aware` but disables vLLM automatic prefix caching (APC).

All 18 conditions and 1,152 formal requests succeeded. The table reports the mean token cache-hit ratio across two seeds and the median of each seed's warm p50 time to first nonempty text token (TTFT). A uses `cache_aware`, B uses `round_robin`, and D uses `cache_aware` with vLLM APC disabled.

| Endpoint | A hit / warm TTFT | B hit / warm TTFT | D warm TTFT |
|---|---:|---:|---:|
| Chat Completions | 68.4% / 55.2 ms | 51.9% / 139.5 ms | 380.6 ms |
| Responses | 69.1% / 51.4 ms | 47.1% / 140.2 ms | 364.3 ms |
| Messages | 68.3% / 51.2 ms | 52.9% / 119.2 ms | 351.4 ms |

Local Codex and Claude requests also checked real Responses and Messages prefix matching. All six seed/shared/control requests succeeded. Repeating the complete input produced 1,438 more matched characters for Codex and 2,382 more for Claude than each control. `matched_chars` is a router text-prefix measure, not actual engine KV hits. These results describe a single-GPU, two-process synthetic workload; they do not measure Mooncake, DP ranks or production tail latency. The raw records and detailed method are in the separately delivered `affinity-experiments/results/h200-9bf6328-2seeds/REPORT.md`.
