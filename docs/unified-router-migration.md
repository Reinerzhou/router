# Unified Router migration on v0.1.17

This branch builds on `feat/v0.1.17-vllm-online`. See `v0.1.17-migration.md` for inherited patches.
The LMDeploy source is organization main `3fca460`; personal main was fast-forwarded to that SHA.

## Interfaces

- `vllm-router` defaults to the vLLM backend; `lmdeploy-router` defaults to LMDeploy.
- `--backend lmdeploy` selects the LMDeploy wire adapter explicitly.
- `--lmdeploy-pd-disaggregation` selects LMDeploy P/D; vLLM P/D flags cannot be combined with it.
  An explicit non-LMDeploy backend with LMDeploy P/D is rejected.
- Historical assistant messages use `reasoning` for vLLM and `reasoning_content` for LMDeploy.
  The LMDeploy HTTP Chat handler prefers a string reasoning_content, including the empty string.
- `--policy sticky_least_loaded --sticky-load-metric sessions|requests` selects the load metric.
  The default is sessions for vLLM and requests for LMDeploy. Request-load sticky requires regular
  mode; it does not enable LMDeploy P/D endpoints or scheduling capabilities.
  The requests choice also keeps the LMDeploy session-key rules: metadata.user_id is accepted,
  and x-request-id/x-trace-id do not create sticky sessions. Sessions retains the upstream rules.
- The sessions metric uses `VLLM_ROUTER_SLL_SESSION_EXPIRATION_IN_S`; requests uses
  `STICKY_LEAST_LOADED_SESSION_EXPIRATION_IN_S`. Both default to 7200 seconds.
- Set `OTEL_SERVICE_NAME=lmdeploy-router` for the LMDeploy model.router span contract.
  JSONL observation is separately opt-in; no inference probes are generated.

Completion byte preservation keeps the official scope: regular HTTP, KV-aware policy and no DP
rewrite. Other policies retain typed serialization and passthrough extension values.

## Commit disposition

| Source | Treatment |
| --- | --- |
| `5d5564c` | Adapt LMDeploy P/D, node registration, token input/output, Python and CLI wiring. |
| `0641016` | Adapt reasoning at backend boundaries; skip already supported Max reasoning effort. |
| `ac17fa5` | Session header inherited from the vLLM migration; retain upstream developer passthrough. |
| `305d8e9` | Keep upstream end-to-end Header handling; do not restore older copying code. |
| `2ed2fa8` | Skip: upstream already uses serde_json preserve_order. |
| `387e1b2` | Keep upstream raw multimodal content blocks; do not replace the protocol wholesale. |
| `e5ddbae`, `7240bc4` | Adapt JSON error normalization while retaining the upstream KV-aware raw completion path. |
| `7312397` | Forward non-success streaming-request responses using their original format. |
| `5ed60f5` | Adapt inbound OTel correlation and coexist with opt-in lifecycle JSONL observation. |
| `2e324f1` | Adapt request-load placement as a choice alongside upstream session-load sticky. |
| `c94acdc` | Preserve Python 3.13 wheel support in the separate binary package. |
| Release version bumps | Skip historical release numbers; the candidate wheel is 0.1.17.dev0. |

The wheel selects only the lmdeploy-router binary, using maturin >= 1.16 target filtering:
https://www.maturin.rs/config#targets

Python also exposes `lmdeploy_backend`, `lmdeploy_pd_disaggregation` and the migration transport
settings through `RouterArgs`. CLI equivalents use `--lmdeploy-*`; `--lmdeploy-disable-gdr`
sets `lmdeploy_with_gdr=False`. Python `sticky_least_loaded_requests` explicitly selects request
load, and the LMDeploy `Router.from_args` wrapper applies the LMDeploy default.

## Validation boundary

Acceptance is compilation and CPU/mock-backend regression. H200 and 910C deployment, model
accuracy, real KV migration and performance are not certified by this migration.
LMDeploy P/D retains its existing Chat/Completions scope; generate and Responses remain unsupported.
No backend vLLM/Ascend source is patched and no release/tag or organization PR is created here.

## Reproduce migration checks

The CPU checks require no model weights or inference GPUs. Keep loopback traffic outside proxies.
The `cpu,ready` features enable the corresponding test modules.

```bash
export NO_PROXY=127.0.0.1,localhost
export no_proxy="$NO_PROXY"
cargo check --locked --all-targets --features cpu,ready
cargo test --locked --no-default-features --features cpu,ready --lib lmdeploy -- --test-threads=1
cargo test --locked --no-default-features --features cpu,ready --test lmdeploy_registration_test -- --test-threads=1
cargo test --locked --no-default-features --features cpu,ready --test otel_excluded_route_test -- --test-threads=1
cargo test --locked --no-default-features --features cpu,ready --test otel_integration_test -- --test-threads=1
cargo test --locked --no-default-features --features cpu,ready --test request_formats_test -- --test-threads=1
cargo test --locked --no-default-features --features cpu,ready --test api_endpoints_test error_tests:: -- --test-threads=1
python3 tests/backend_migration_smoke.py target/debug/vllm-router --expected-backend vllm
python3 tests/backend_migration_smoke.py target/debug/lmdeploy-router --expected-backend lmdeploy
python3 tests/backend_migration_smoke.py target/debug/lmdeploy-router --expected-backend lmdeploy --lmdeploy-pd
ROUTER_BINARY=target/debug/vllm-router python3 tests/test_nixl_checksum_forwarding.py
```

Additional library filters used for migration are `protocols::`, `policies::sticky_least_loaded::`,
`policies::hash_key::`, `config::`, `routers::http::lmdeploy_pd_router::`,
`server::tests::completion`,
`routers::http::router::tests::test_streaming_generation_preserves_upstream_errors`,
`otel_trace::` and `observability::`. Run them in separate test processes with one test thread.

Build the standalone candidate wheel from its package directory:

```bash
cd packaging/lmdeploy-router
maturin build --profile dev --compatibility linux --features cpu,ready --out dist
```

This command checks native Linux packaging only. A formal release still needs an optimized build
in the supported manylinux environment, its compatibility audit, and backend acceptance testing.
The candidate package version is independent of the Rust baseline; do not publish it as an existing
LMDeploy release. Only the `lmdeploy-router` executable belongs in this wheel.

## Validation results

| Check | Result |
| --- | --- |
| Compilation | All-target interface check with `cpu,ready`; both executable entrypoints built. |
| Focused library regression | 318 tests passed across protocols, sticky/hash, configuration, P/D, errors and observation. |
| HTTP integration | 17 tests passed: registration (2), excluded tracing (1), full tracing (1), request formats (7), API errors (6). |
| Actual executable smoke | Both default backends, both explicit overrides, LMDeploy P/D mock, and both KV-aware raw-completion paths passed. |
| Inherited vLLM checks | 4 checksum/error tests, 12 native-observation requests, and 14 multi-endpoint requests passed. |
| Candidate wheel | Native Linux dev wheel rebuilt after the tracing fix; sole LMDeploy binary, Python 3.13.5 reinstall and installed HTTP smoke passed. |

The tracing regression exercises transparent `/v1/messages` with a query string. Its fixed route
name is recorded without including the query or arbitrary unmatched paths, and LMDeploy retains
its `model.router` span name. Backend request counts in the observation checks remain 24 and 28;
the router does not generate extra inference probes.
