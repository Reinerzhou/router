#!/usr/bin/env python3
"""Exercise the real Router PD path using deterministic P/D HTTP fixtures."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import argparse
import base64
import hashlib
import hmac
from datetime import datetime, timezone
import os
import re
from pathlib import Path
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(0.03)
    raise AssertionError("native integration wait timed out")


def run(
    binary,
    output_dir,
    retention_days=None,
    rust_log="warn",
    log_level="info",
    native_file=False,
    role=None,
):
    run_dir = (
        output_dir.resolve()
        if output_dir
        else Path(tempfile.mkdtemp(prefix="router-native-"))
    )
    if output_dir:
        run_dir.mkdir(parents=True)
    binary = binary.resolve()
    if not binary.is_file():
        raise FileNotFoundError(binary)
    posts = []
    alerts = []
    terminal_release = threading.Event()

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            assert data["msg_type"] == "text"
            expected = base64.b64encode(
                hmac.new(
                    (data["timestamp"] + "\nrouter-fixture-secret").encode(),
                    b"",
                    hashlib.sha256,
                ).digest()
            ).decode()
            assert data["sign"] == expected
            assert "[推理告警]" in data["content"]["text"]
            alerts.append(data)
            with (run_dir / "webhooks.jsonl").open("a") as f:
                f.write(json.dumps(data) + "\n")
            self.send_response(200)
            self.end_headers()
            self.wfile.write(
                json.dumps({"code": 19021 if len(alerts) == 1 else 0}).encode()
            )

        def log_message(self, *args):
            pass

    receiver = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    threading.Thread(target=receiver.serve_forever, daemon=True).start()

    class Backend(BaseHTTPRequestHandler):
        def do_GET(self):
            data = {
                "data": [{"id": "native-fixture", "object": "model"}],
                "model_path": "native-fixture",
                "tp_size": 1,
                "dp_size": 1,
            }
            raw = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            mode = payload["messages"][0]["content"]
            posts.append(
                {
                    "role": self.server.role,
                    "client_id": self.headers.get("X-Client-Request-Id"),
                    "backend_id": self.headers.get("X-Request-Id"),
                    "observation_id": self.headers.get("X-Observability-Request-Id"),
                }
            )
            if self.server.role == "prefill":
                body = json.dumps(
                    {
                        "id": "prefill",
                        "choices": [{"message": {"content": "p"}}],
                        "kv_transfer_params": {
                            "do_remote_prefill": True,
                            "remote_engine_id": "fixture",
                        },
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if mode == "nonstream":
                body = json.dumps(
                    {
                        "choices": [
                            {"message": {"reasoning": "hidden", "content": "hello"}}
                        ],
                        "usage": {"completion_tokens_details": {"reasoning_tokens": 8}},
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            try:
                self.wfile.write(
                    b'data: {"choices":[{"delta":{"role":"assistant"}}],"error":null}\n\n'
                )
                self.wfile.flush()
                if mode == "stall":
                    time.sleep(0.8)
                if mode.startswith("reasoning"):
                    reasoning_field = (
                        "reasoning_content"
                        if mode == "reasoning_content"
                        else "reasoning"
                    )
                    for thought in ("", "thinking begins", "thinking continues"):
                        self.wfile.write(
                            b"data: "
                            + json.dumps(
                                {"choices": [{"delta": {reasoning_field: thought}}]}
                            ).encode()
                            + b"\n\n"
                        )
                        self.wfile.flush()
                if mode in ("error", "reasoning-error"):
                    self.wfile.write(
                        b'data: {"error":{"message":"fixture failure"}}\n\n'
                    )
                else:
                    self.wfile.write(
                        b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n'
                    )
                    self.wfile.flush()
                if mode in ("cancel", "reasoning-cancel"):
                    for _ in range(20):
                        time.sleep(0.05)
                        self.wfile.write(
                            b'data: {"choices":[{"delta":{"content":"."}}]}\n\n'
                        )
                        self.wfile.flush()
                if mode in (
                    "reasoning",
                    "reasoning_content",
                    "reasoning-terminal-drop",
                ):
                    self.wfile.write(
                        b'data: {"choices":[],"usage":{"completion_tokens_details":{"reasoning_tokens":12}}}\n\n'
                    )
                if mode not in ("truncated", "reasoning-truncated"):
                    self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                if mode == "reasoning-terminal-drop":
                    terminal_release.wait(timeout=10)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    servers = []
    for backend_role in ("prefill", "decode"):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
        server.role = backend_role
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
    router_port, metrics_port = port(), port()
    with tempfile.TemporaryDirectory(prefix="native-router-private-") as temp:
        secret = Path(temp) / "feishu.json"
        secret.write_text(
            json.dumps(
                {
                    "url": f"http://127.0.0.1:{receiver.server_port}/alerts",
                    "secret": "router-fixture-secret",
                    "slow_request_seconds": 0.05,
                }
            )
        )
        secret.chmod(0o600)
        env = {
            **os.environ,
            "OBSERVABILITY_SERVICE": "native-router-fixture",
            "OBSERVABILITY_ALERT_CONFIG": str(secret),
            "OBSERVABILITY_LOG_DIR": str(run_dir / "logs"),
            "OBSERVABILITY_ENABLED": "1",
            "RUST_LOG": rust_log,
            "OBSERVABILITY_LOG_MAX_MB": ".01",
            "OBSERVABILITY_LOG_INTERVAL_S": ".1",
        }
        env.pop("OBSERVABILITY_ROLE", None)
        if role is not None:
            env["OBSERVABILITY_ROLE"] = role
        env.pop("OBSERVABILITY_LOG_RETENTION_DAYS", None)
        if retention_days is not None:
            env["OBSERVABILITY_LOG_RETENTION_DAYS"] = retention_days
        owned = (
            run_dir / "logs" / "router" / (env.get("HOSTNAME") or socket.gethostname())
        )
        if role and role != "router":
            owned /= role
        owned.mkdir(parents=True)
        expired = owned / "20000101T000000.000000Z.1.jsonl"
        expired.write_text("expired")
        os.utime(expired, (time.time() - 8 * 86400,) * 2)
        legacy_dir = owned / "router" if role in (None, "router") else owned
        legacy_dir.mkdir(exist_ok=True)
        legacy = legacy_dir / (
            "vllm-router.20000101T000000.000000Z.1." + "a" * 32 + ".jsonl"
        )
        legacy.write_text("expired legacy")
        os.utime(legacy, (time.time() - 8 * 86400,) * 2)
        manual_json = owned / "manual.jsonl"
        manual_json.write_text("unrelated")
        os.utime(manual_json, (time.time() - 8 * 86400,) * 2)
        manual = owned / "manual.keep"
        manual.write_text("keep")
        os.utime(manual, (time.time() - 8 * 86400,) * 2)
        output = (run_dir / "console.log").open("wb")
        proc = subprocess.Popen(
            [
                str(binary),
                *(["--log-dir", str(run_dir / "native-output")] if native_file else []),
                "--log-level",
                log_level,
                "--vllm-pd-disaggregation",
                "--kv-connector",
                "nixl",
                "--policy",
                "round_robin",
                "--prefill",
                f"http://127.0.0.1:{servers[0].server_port}",
                "--decode",
                f"http://127.0.0.1:{servers[1].server_port}",
                "--host",
                "127.0.0.1",
                "--port",
                str(router_port),
                "--prometheus-host",
                "127.0.0.1",
                "--prometheus-port",
                str(metrics_port),
                "--worker-startup-check-interval",
                "1",
            ],
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def records():
            rows = []
            for p in owned.glob("*.jsonl"):
                for line in p.read_text().splitlines():
                    try:
                        rows.append(json.loads(line))
                    except ValueError:
                        pass
            return rows

        def request(mode):
            req = urllib.request.Request(
                f"http://127.0.0.1:{router_port}/v1/chat/completions",
                data=json.dumps(
                    {
                        "model": "native-fixture",
                        "messages": [{"role": "user", "content": mode}],
                        "stream": mode != "nonstream",
                        "max_tokens": 8,
                    }
                ).encode(),
                headers={
                    "Content-Type": "application/json",
                    "X-Request-Id": "client-" + mode,
                },
            )
            if mode == "reasoning-terminal-drop":
                with socket.create_connection(
                    ("127.0.0.1", router_port), timeout=10
                ) as connection:
                    body = req.data
                    connection.sendall(
                        (
                            f"POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                            f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
                            f"X-Request-Id: client-{mode}\r\n\r\n"
                        ).encode()
                        + body
                    )
                    received = b""
                    while b"[DONE]" not in received:
                        data = connection.recv(4096)
                        assert data, "connection ended before the terminal event"
                        received += data
                    connection.setsockopt(
                        socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                    )
                return
            response = opener.open(req, timeout=10)
            if mode in ("cancel", "reasoning-cancel"):
                while b"hello" not in response.readline():
                    pass
                response.close()
                return
            with response:
                return response.read()

        try:
            wait(
                lambda: opener.open(
                    f"http://127.0.0.1:{router_port}/health", timeout=1
                ).read(),
                30,
            )
            for mode in (
                "fast",
                "truncated",
                "error",
                "reasoning",
                "reasoning_content",
                "reasoning-error",
                "reasoning-truncated",
            ):
                raw = request(mode)
                assert raw.startswith(
                    b'data: {"choices":[{"delta":{"role":"assistant"}}],"error":null}\n\n'
                )
                if mode in ("reasoning", "reasoning_content"):
                    expected = b'data: {"choices":[{"delta":{"role":"assistant"}}],"error":null}\n\n'
                    for thought in ("", "thinking begins", "thinking continues"):
                        expected += (
                            b"data: "
                            + json.dumps(
                                {"choices": [{"delta": {mode: thought}}]}
                            ).encode()
                            + b"\n\n"
                        )
                    expected += b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n'
                    expected += b'data: {"choices":[],"usage":{"completion_tokens_details":{"reasoning_tokens":12}}}\n\n'
                    expected += b"data: [DONE]\n\n"
                    assert raw == expected, (
                        "observability must preserve exact SSE bytes"
                    )
                if mode not in ("truncated", "reasoning-truncated"):
                    assert raw.endswith(b"data: [DONE]\n\n")
            pending = threading.Thread(target=lambda: request("stall"))
            pending.start()
            wait(
                lambda: any(
                    "状态: firing" in a["content"]["text"]
                    and "request_e2e_slow" in a["content"]["text"]
                    for a in alerts
                )
            )
            assert pending.is_alive(), (
                "slow-request alarm must arrive before the actual stream completes"
            )
            pending.join(timeout=10)
            assert not pending.is_alive()
            wait(
                lambda: any(
                    "状态: resolved" in a["content"]["text"]
                    and "request_e2e_slow" in a["content"]["text"]
                    for a in alerts
                )
            )
            request("cancel")
            request("reasoning-cancel")
            request("reasoning-terminal-drop")
            wait(
                lambda: any(
                    r.get("event") == "request_finished"
                    and r.get("client_request_id") == "client-reasoning-terminal-drop"
                    for r in records()
                )
            )
            terminal_release.set()
            assert (
                json.loads(request("nonstream"))["usage"]["completion_tokens_details"][
                    "reasoning_tokens"
                ]
                == 8
            )
            wait(
                lambda: any(
                    r.get("event") == "request_finished"
                    and r.get("client_request_id") == "client-cancel"
                    and r.get("outcome") == "cancelled"
                    for r in records()
                )
            )
            wait(
                lambda: (
                    len([r for r in records() if r.get("event") == "request_finished"])
                    >= 12
                )
            )
            rows = records()
            snapshots = [r for r in rows if r.get("event") == "observability_status"]
            assert snapshots
            assert all(
                r["log_io_errors"] == 0 and r["alert_dropped"] == 0 for r in snapshots
            )
            assert max(r["webhook_errors"] for r in snapshots) >= 1
            for row in rows:
                ns = row["timestamp_unix_ns"]
                expected = (
                    datetime.fromtimestamp(ns // 10**9, timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S."
                    )
                    + f"{ns // 10**6 % 1000:03d}Z"
                )
                assert row["timestamp"] == expected
                assert "observation_id" not in row
                assert row["role"] == (role or "router"), (
                    "configured role must survive backend setup"
                )
            assert not any(
                r.get("event")
                in (
                    "request_first_output_slow",
                    "response_headers_slow",
                    "request_output_stalled",
                )
                for r in rows
            )
            assert (
                len(
                    [
                        a
                        for a in alerts
                        if "request_e2e_slow" in a["content"]["text"]
                        and "状态: firing" in a["content"]["text"]
                    ]
                )
                == 1
            )
            assert len(alerts) >= 2 and alerts[0]["content"] == alerts[1]["content"], (
                "HTTP-200 business failure must be retried"
            )
            finished = {
                r["client_request_id"]: r
                for r in rows
                if r.get("event") == "request_finished"
            }
            assert finished["client-fast"]["outcome"] == "success"
            assert finished["client-fast"]["terminal_reason"] == "stream_completed"
            assert finished["client-fast"]["streaming"] is True
            terminal = finished["client-reasoning-terminal-drop"]
            assert terminal["outcome"] == "success", terminal
            assert terminal["terminal_reason"] == "stream_completed"
            assert terminal["protocol_done"] is True
            assert terminal["reasoning_tokens"] == 12
            assert terminal["reasoning_tokens_complete"] is True
            assert finished["client-truncated"]["terminal_reason"] == "stream_truncated"
            assert (
                finished["client-error"]["terminal_reason"] == "stream_protocol_error"
            )
            assert finished["client-cancel"]["terminal_reason"] == "cancelled"
            assert finished["client-truncated"]["outcome"] == "error"
            assert finished["client-error"]["outcome"] == "error"
            assert finished["client-stall"]["e2e_seconds"] > 0.75
            assert finished["client-stall"]["ttft_seconds"] > 0.75
            for mode, end in finished.items():
                assert all(
                    k in end
                    for k in (
                        "reasoning_first_seconds",
                        "reasoning_last_seconds",
                        "content_first_seconds",
                        "reasoning_to_content_seconds",
                        "reasoning_tokens",
                        "reasoning_tokens_complete",
                    )
                )
                if mode not in (
                    "client-reasoning",
                    "client-reasoning_content",
                    "client-nonstream",
                    "client-reasoning-terminal-drop",
                ):
                    assert end["reasoning_tokens"] is None
                    assert end["reasoning_tokens_complete"] is False
                if not mode.startswith("client-reasoning"):
                    assert end["reasoning_first_seconds"] is None
            for mode in ("reasoning-error", "reasoning-truncated", "reasoning-cancel"):
                end = finished["client-" + mode]
                assert end["reasoning_first_seconds"] is not None
                assert end["reasoning_last_seconds"] >= end["reasoning_first_seconds"]
                assert end["outcome"] == (
                    "cancelled" if mode.endswith("cancel") else "error"
                )
            end = finished["client-nonstream"]
            assert (
                end["reasoning_tokens"] == 8
                and end["reasoning_tokens_complete"] is True
            )
            assert end["content_first_seconds"] is None
            for mode in ("reasoning", "reasoning_content"):
                end = finished["client-" + mode]
                assert (
                    0
                    <= end["reasoning_first_seconds"]
                    <= end["reasoning_last_seconds"]
                    <= end["content_first_seconds"]
                )
                assert (
                    abs(
                        end["reasoning_to_content_seconds"]
                        - (
                            end["content_first_seconds"]
                            - end["reasoning_first_seconds"]
                        )
                    )
                    < 1e-9
                )
                assert end["reasoning_tokens"] == 12
                assert end["reasoning_tokens_complete"] is True
                assert finished["client-" + mode]["output_chunks"] == 3
                assert finished["client-" + mode]["outcome"] == "success"
            for mode in (
                "fast",
                "truncated",
                "error",
                "stall",
                "cancel",
                "reasoning",
                "reasoning_content",
                "reasoning-error",
                "reasoning-truncated",
                "reasoning-cancel",
                "nonstream",
                "reasoning-terminal-drop",
            ):
                pair = [p for p in posts if p["client_id"] == "client-" + mode]
                assert len(pair) == 2 and pair[0]["backend_id"] == pair[1]["backend_id"]
                assert pair[0]["backend_id"].startswith("___prefill_addr_")
                assert pair[0]["observation_id"] == pair[1]["observation_id"]
            count = len(posts)
            time.sleep(0.3)
            assert len(posts) == count == 24, "no synthetic inference traffic"
            assert expired.exists() == (retention_days != "7")
            assert manual.exists() and manual_json.exists()
            assert legacy.exists() == (retention_days != "7")
            generated = [
                p
                for p in owned.glob("*.jsonl")
                if p not in (expired, legacy, manual_json)
            ]
            assert all(
                re.fullmatch(
                    r"[0-9]{8}T[0-9]{6}\.[0-9]{6}Z\.[1-9][0-9]*(?:\.[1-9][0-9]*)?\.jsonl",
                    p.name,
                )
                for p in generated
            )
            assert len(generated) > 1
            assert len(list(owned.glob("*.jsonl"))) > 1
            metrics = (
                opener.open(f"http://127.0.0.1:{metrics_port}/metrics").read().decode()
            )
            (run_dir / "metrics.txt").write_text(metrics)
            assert "vllm_router_response_complete_seconds" in metrics
            output.flush()
            assert not (run_dir / "logs" / "console").exists()
            assert not list((run_dir / "logs").rglob("*.log"))
            assert not list((run_dir / "logs").rglob("*.meta.json"))
            assert all(
                r.get("target", "").startswith("vllm_router_rs::observability")
                for r in rows
            ), "native framework tracing must not enter lifecycle JSONL"
            if rust_log == "info":
                assert "Starting server on" in (run_dir / "console.log").read_text(), (
                    "native INFO output must survive enabling lifecycle JSONL"
                )
            assert "request_finished" not in (run_dir / "console.log").read_text(), (
                "lifecycle JSONL must not duplicate request events into native output"
            )
            if native_file:
                native_logs = list((run_dir / "native-output").glob("*"))
                assert native_logs
                native_text = "".join(p.read_text() for p in native_logs)
                assert "Starting server on" in native_text
                assert "request_finished" not in native_text
            (run_dir / "backend-requests.json").write_text(
                json.dumps(posts, indent=2) + "\n"
            )
            summary = {
                "passed": True,
                "pd_requests": len(posts) // 2,
                "reasoning_field_compatibility": True,
                "backend_requests": count,
                "id_mapping": True,
                "full_stream_timing": True,
                "cancel_classified": True,
                "terminal_drop_preserves_success_and_final_usage": True,
                "truncation_and_sse_error_detected": True,
                "webhook_before_completion": True,
                "webhook_recovery": True,
                "feishu_signed": True,
                "feishu_business_error_retry": True,
                "unified_timestamps": True,
                "minimal_configuration": True,
                "jsonl_only": True,
                "native_stdout_independent": True,
                "native_file_independent": native_file,
                "removed_noisy_alerts": True,
                "rotation_and_retention": True,
                "configured_role": role,
                "effective_role": role or "router",
                "directory": str(owned.relative_to(run_dir / "logs")),
                "retention_days": retention_days or "default_manual",
                "old_log_preserved": expired.exists(),
                "no_synthetic_inference": True,
            }
            (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(run_dir)
            print(json.dumps(summary))
        finally:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
            output.close()
            for server in [*servers, receiver]:
                server.shutdown()
                server.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--router-binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--retention-days", choices=("0", "7"))
    parser.add_argument("--native-file", action="store_true")
    parser.add_argument("--role")
    parser.add_argument("--rust-log", default="warn")
    parser.add_argument(
        "--log-level", choices=("info", "warn", "error"), default="info"
    )
    args = parser.parse_args()
    run(
        args.router_binary,
        args.output_dir,
        args.retention_days,
        args.rust_log,
        args.log_level,
        args.native_file,
        args.role,
    )
