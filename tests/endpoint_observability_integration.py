"""Check all observed endpoint protocols through the real Router and mock P/D."""

import argparse
import json
import os
import signal
import subprocess
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from native_observability_integration import port, wait

CASES_PATH = Path(__file__).with_name("fixtures") / "native_observability_cases.json"


def wire(case):
    if not case["streaming"]:
        return json.dumps(case["events"][-1]["data"]).encode()
    data = b"".join(
        b"data: " + json.dumps(item["data"]).encode() + b"\n\n"
        for item in case["events"]
    )
    if case["route"] == "/v1/chat/completions":
        data += b"data: [DONE]\n\n"
    return data


def check_summary(end, case):
    assert end["outcome"] == ("success" if case["success"] else "error"), end
    for name, expected in case["expected"].items():
        if name == "first_output_seconds":
            assert (end["ttft_seconds"] is None) == (expected is None), end
        elif name in ("reasoning_tokens", "reasoning_tokens_complete"):
            assert end[name] == expected, (case["name"], name, end)
        else:
            assert (end[name] is None) == (expected is None), (case["name"], name, end)


def run(binary, output_dir):
    cases = json.loads(CASES_PATH.read_text())
    by_name = {c["name"]: c for c in cases}
    output_dir.mkdir(parents=True)
    posts = []

    class Backend(BaseHTTPRequestHandler):
        def do_GET(self):
            data = json.dumps(
                {
                    "data": [{"id": "native-fixture"}],
                    "model_path": "native-fixture",
                    "tp_size": 1,
                    "dp_size": 1,
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            case = by_name[payload["metadata"]["user_id"]]
            posts.append(
                {
                    "case": case["name"],
                    "role": self.server.role,
                    "request_id": self.headers.get("X-Observability-Request-Id"),
                }
            )
            if self.server.role == "prefill":
                data = json.dumps(
                    {
                        "id": "prefill",
                        "choices": [{"message": {"content": "p"}}],
                        "kv_transfer_params": {
                            "do_remote_prefill": True,
                            "remote_engine_id": "fixture",
                        },
                    }
                ).encode()
            else:
                data = wire(case)
            self.send_response(200)
            streaming = self.server.role == "decode" and case["streaming"]
            self.send_header(
                "Content-Type", "text/event-stream" if streaming else "application/json"
            )
            if not streaming:
                self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            for offset in range(0, len(data), 17):
                self.wfile.write(data[offset : offset + 17])
                self.wfile.flush()

        def log_message(self, *args):
            pass

    servers = []
    process = None
    try:
        for role in ("prefill", "decode"):
            server = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
            server.role = role
            threading.Thread(target=server.serve_forever, daemon=True).start()
            servers.append(server)
        router_port = port()
        env = {
            k: v for k, v in os.environ.items() if not k.startswith("OBSERVABILITY_")
        }
        env.update(
            OBSERVABILITY_ENABLED="1",
            OBSERVABILITY_LOG_DIR=str(output_dir / "logs"),
            RUST_LOG="off",
        )
        with (output_dir / "console.log").open("w") as console:
            process = subprocess.Popen(
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
                    str(port()),
                    "--worker-startup-check-interval",
                    "1",
                    "--log-level",
                    "error",
                ],
                env=env,
                stdout=console,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            wait(
                lambda: opener.open(
                    f"http://127.0.0.1:{router_port}/health", timeout=1
                ).read(),
                30,
            )
            for case in cases:
                payload = {
                    "model": "native-fixture",
                    "stream": case["streaming"],
                    "metadata": {"user_id": case["name"]},
                    "max_tokens": 8,
                }
                if case["route"] == "/v1/responses":
                    payload.update(input="fixture", max_output_tokens=8)
                else:
                    payload["messages"] = [{"role": "user", "content": "fixture"}]
                request = urllib.request.Request(
                    f"http://127.0.0.1:{router_port}{case['route']}",
                    data=json.dumps(payload).encode(),
                    headers={
                        "Content-Type": "application/json",
                        "X-Request-Id": "client-" + case["name"],
                    },
                )
                with opener.open(request, timeout=10) as response:
                    raw = response.read()
                    if case["streaming"]:
                        assert raw == wire(case), case["name"]
                    else:
                        assert json.loads(raw) == json.loads(wire(case)), case["name"]

            def records():
                rows = []
                for path in (output_dir / "logs").rglob("*.jsonl"):
                    for line in path.read_text().splitlines():
                        try:
                            rows.append(json.loads(line))
                        except ValueError:
                            pass
                return rows

            wait(
                lambda: (
                    len([r for r in records() if r.get("event") == "request_finished"])
                    == len(cases)
                )
            )
            ends = {
                r["client_request_id"]: r
                for r in records()
                if r.get("event") == "request_finished"
            }
            for case in cases:
                end = ends["client-" + case["name"]]
                check_summary(end, case)
                pair = [p for p in posts if p["case"] == case["name"]]
                assert (
                    len(pair) == 2
                    and pair[0]["request_id"]
                    == pair[1]["request_id"]
                    == end["request_id"]
                )
            (output_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "passed": True,
                        "requests": len(cases),
                        "backend_requests": len(posts),
                        "stream_bytes_unchanged": True,
                        "nonstream_json_preserved": True,
                        "endpoint_summaries": True,
                    },
                    indent=2,
                )
                + "\n"
            )
    finally:
        if process is not None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
        for server in servers:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    run(args.binary.resolve(), args.output.resolve())
