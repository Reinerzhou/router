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
from pathlib import Path
import signal
import socket
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


def run(binary, output_dir, retention_days=None):
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
                if mode == "error":
                    self.wfile.write(
                        b'data: {"error":{"message":"fixture failure"}}\n\n'
                    )
                else:
                    self.wfile.write(
                        b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n'
                    )
                    self.wfile.flush()
                if mode == "cancel":
                    for _ in range(20):
                        time.sleep(0.05)
                        self.wfile.write(
                            b'data: {"choices":[{"delta":{"content":"."}}]}\n\n'
                        )
                        self.wfile.flush()
                if mode != "truncated":
                    self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    servers = []
    for role in ("prefill", "decode"):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
        server.role = role
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
            "OBSERVABILITY_ROLE": "router",
            "OBSERVABILITY_ALERT_CONFIG": str(secret),
            "OBSERVABILITY_LOG_DIR": str(run_dir / "logs"),
            "OBSERVABILITY_ENABLED": "1",
            "RUST_LOG": "warn",
            "OBSERVABILITY_LOG_MAX_MB": ".01",
            "OBSERVABILITY_LOG_INTERVAL_S": ".1",
        }
        env.pop("OBSERVABILITY_LOG_RETENTION_DAYS", None)
        if retention_days is not None:
            env["OBSERVABILITY_LOG_RETENTION_DAYS"] = retention_days
        owned = (
            run_dir
            / "logs"
            / "router"
            / (env.get("HOSTNAME") or socket.gethostname())
            / "router"
        )
        owned.mkdir(parents=True)
        expired = owned / "vllm-router.old.jsonl"
        expired.write_text("expired")
        os.utime(expired, (time.time() - 8 * 86400,) * 2)
        manual = owned / "manual.keep"
        manual.write_text("keep")
        os.utime(manual, (time.time() - 8 * 86400,) * 2)
        output = (run_dir / "console.log").open("wb")
        proc = subprocess.Popen(
            [
                str(binary),
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
            for p in (run_dir / "logs").rglob("vllm-router.*.jsonl"):
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
                        "stream": True,
                        "max_tokens": 8,
                    }
                ).encode(),
                headers={
                    "Content-Type": "application/json",
                    "X-Request-Id": "client-" + mode,
                },
            )
            response = opener.open(req, timeout=10)
            if mode == "cancel":
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
            for mode in ("fast", "truncated", "error"):
                request(mode)
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
            wait(
                lambda: any(
                    r.get("event") == "request_finished"
                    and r.get("client_request_id") == "client-cancel"
                    and r.get("outcome") == "cancelled"
                    for r in records()
                )
            )
            wait(
                lambda: len(
                    [r for r in records() if r.get("event") == "request_finished"]
                )
                >= 5
            )
            rows = records()
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
            assert finished["client-truncated"]["terminal_reason"] == "stream_truncated"
            assert (
                finished["client-error"]["terminal_reason"] == "stream_protocol_error"
            )
            assert finished["client-cancel"]["terminal_reason"] == "cancelled"
            assert finished["client-truncated"]["outcome"] == "error"
            assert finished["client-error"]["outcome"] == "error"
            assert finished["client-stall"]["e2e_seconds"] > 0.75
            assert finished["client-stall"]["ttft_seconds"] > 0.75
            for mode in ("fast", "truncated", "error", "stall", "cancel"):
                pair = [p for p in posts if p["client_id"] == "client-" + mode]
                assert len(pair) == 2 and pair[0]["backend_id"] == pair[1]["backend_id"]
                assert pair[0]["backend_id"].startswith("___prefill_addr_")
                assert pair[0]["observation_id"] == pair[1]["observation_id"]
            count = len(posts)
            time.sleep(0.3)
            assert len(posts) == count == 10, "no synthetic inference traffic"
            assert expired.exists() == (retention_days != "7")
            assert manual.exists()
            assert len(list(owned.glob("vllm-router.*.jsonl"))) > 1
            metrics = (
                opener.open(f"http://127.0.0.1:{metrics_port}/metrics").read().decode()
            )
            (run_dir / "metrics.txt").write_text(metrics)
            assert "vllm_router_response_complete_seconds" in metrics
            output.flush()
            assert "request_finished" not in (run_dir / "console.log").read_text(), (
                "console suppression must preserve file events without duplicate console events"
            )
            (run_dir / "backend-requests.json").write_text(
                json.dumps(posts, indent=2) + "\n"
            )
            summary = {
                "passed": True,
                "pd_requests": 5,
                "backend_requests": count,
                "id_mapping": True,
                "full_stream_timing": True,
                "cancel_classified": True,
                "truncation_and_sse_error_detected": True,
                "webhook_before_completion": True,
                "webhook_recovery": True,
                "feishu_signed": True,
                "feishu_business_error_retry": True,
                "unified_timestamps": True,
                "minimal_configuration": True,
                "removed_noisy_alerts": True,
                "rotation_and_retention": True,
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
    args = parser.parse_args()
    run(args.router_binary, args.output_dir, args.retention_days)
