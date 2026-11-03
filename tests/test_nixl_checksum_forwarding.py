"""Run the actual router against external P/D HTTP stubs; no GPUs needed."""

import base64
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class TestNixlChecksumForwarding(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.params = {}
        cls.received = []
        cls.p_status = 200
        cls.d_status = 200
        cls.profile_calls = []
        cls.p_disconnect = False
        cls.d_disconnect = False
        cls.servers = []
        for role in ("P", "D"):

            class Handler(BaseHTTPRequestHandler):
                worker_role = role

                def log_message(self, *args):
                    pass

                def do_GET(self):
                    self.respond(200, {"data": [{"id": "test-model"}]})

                def respond(self, status, body):
                    data = json.dumps(body).encode()
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

                def do_POST(self):
                    if self.path in ("/start_profile", "/stop_profile"):
                        cls.profile_calls.append((self.worker_role, self.path))
                        self.respond(200, {})
                        return
                    body = json.loads(
                        self.rfile.read(int(self.headers["Content-Length"]))
                    )
                    disconnect = (
                        cls.p_disconnect
                        if self.worker_role == "P"
                        else cls.d_disconnect
                    )
                    if disconnect:
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    if self.worker_role == "P":
                        if cls.p_status != 200:
                            self.respond(
                                cls.p_status,
                                {
                                    "error": {
                                        "message": "context length exceeded: review marker",
                                        "code": "prefill_test_error",
                                    }
                                },
                            )
                            return
                        self.respond(
                            cls.p_status,
                            {
                                "id": "prefill",
                                "object": "chat.completion",
                                "choices": [
                                    {
                                        "index": 0,
                                        "message": {"role": "assistant", "content": ""},
                                        "finish_reason": "stop",
                                    }
                                ],
                                "kv_transfer_params": cls.params,
                            },
                        )
                    else:
                        cls.received.append(body)
                        if cls.d_status != 200:
                            self.respond(
                                cls.d_status,
                                {
                                    "error": {
                                        "message": "KV load failed",
                                        "type": "server_error",
                                        "code": "kv_load_failure",
                                    }
                                },
                            )
                        else:
                            self.respond(
                                200,
                                {
                                    "id": "decode",
                                    "choices": [
                                        {
                                            "index": 0,
                                            "message": {
                                                "role": "assistant",
                                                "content": "ok",
                                            },
                                            "finish_reason": "stop",
                                        }
                                    ],
                                },
                            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            cls.servers.append(server)
        cls.port = free_port()
        binary = Path(
            os.environ.get(
                "ROUTER_BINARY",
                Path(__file__).resolve().parents[1] / "target/release/vllm-router",
            )
        )
        with tempfile.TemporaryFile() as log:
            cls.process = subprocess.Popen(
                [
                    str(binary),
                    "--vllm-pd-disaggregation",
                    "--prefill",
                    f"http://127.0.0.1:{cls.servers[0].server_port}",
                    "--decode",
                    f"http://127.0.0.1:{cls.servers[1].server_port}",
                    "--port",
                    str(cls.port),
                    "--prometheus-port",
                    str(free_port()),
                    "--policy",
                    "round_robin",
                    "--kv-connector",
                    "nixl",
                    "--disable-retries",
                    "--profile",
                    "--disable-circuit-breaker",
                    "--worker-startup-check-interval",
                    "1",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            deadline = time.monotonic() + 30
            ready = threading.Event()
            while time.monotonic() < deadline:
                if cls.process.poll() is not None:
                    break
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{cls.port}/health", timeout=1
                    ) as response:
                        if response.status == 200:
                            return
                except (OSError, urllib.error.HTTPError):
                    ready.wait(0.05)
            cls.process.terminate()
            cls.process.wait(timeout=10)
            log.seek(0)
            raise RuntimeError(log.read().decode())

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate()
        cls.process.wait(timeout=10)
        for server in cls.servers:
            server.shutdown()
            server.server_close()

    def setUp(self):
        type(self).received.clear()
        type(self).p_status = 200
        type(self).d_status = 200
        type(self).p_disconnect = False
        type(self).d_disconnect = False
        type(self).profile_calls.clear()

    def request(self, endpoint="chat/completions"):
        body = {
            "model": "test-model",
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 8,
            "stream": False,
        }
        if endpoint == "responses":
            body.pop("messages")
            body["input"] = "hello"
            body["max_output_tokens"] = body.pop("max_tokens")
        elif endpoint == "messages":
            body["max_tokens"] = 8
        data = json.dumps(body).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/v1/{endpoint}",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, error.read().decode()

    def test_opaque_checksum_is_forwarded_without_rewriting(self):
        for endpoint in ("chat/completions", "responses", "messages"):
            for checksum in (None, base64.b64encode(bytes(range(256)) * 1024).decode()):
                with self.subTest(
                    endpoint=endpoint, checksum_present=checksum is not None
                ):
                    type(self).params = {
                        "do_remote_prefill": True,
                        "remote_engine_id": "producer",
                        "remote_block_ids": [[1, 2], [7, 8]],
                    }
                    if checksum is not None:
                        self.params["kv_checksums"] = checksum
                    status, body = self.request(endpoint)
                    self.assertEqual(status, 200, body)
                    self.assertEqual(
                        self.received[-1]["kv_transfer_params"], self.params
                    )

    def test_prefill_failure_does_not_dispatch_decode(self):
        for status_code in (400, 413, 429, 500, 503):
            type(self).p_status = status_code
            for endpoint in ("chat/completions", "responses", "messages"):
                with self.subTest(endpoint=endpoint, status=status_code):
                    status, body = self.request(endpoint)
                    self.assertEqual(status, status_code, body)
                    self.assertEqual(
                        json.loads(body)["error"]["message"],
                        "context length exceeded: review marker",
                    )
                    self.assertEqual(self.received, [])
        type(self).p_status = 200
        status, body = self.request()
        self.assertEqual(status, 200, body)
        self.assertEqual(len(self.received), 1)

    def test_transport_errors_stop_profiling_before_returning(self):
        for role in ("P", "D"):
            with self.subTest(role=role):
                type(self).profile_calls.clear()
                type(self).p_disconnect = role == "P"
                type(self).d_disconnect = role == "D"
                status, body = self.request()
                self.assertGreaterEqual(status, 500, body)
                self.assertIn((role, "/start_profile"), self.profile_calls)
                self.assertIn((role, "/stop_profile"), self.profile_calls)
        type(self).p_disconnect = False
        type(self).d_disconnect = False
        status, body = self.request()
        self.assertEqual(status, 200, body)

    def test_decode_failure_is_not_a_success_and_next_request_works(self):
        type(self).params = {"do_remote_prefill": True, "kv_checksums": "opaque"}
        type(self).d_status = 500
        status, body = self.request()
        self.assertGreaterEqual(status, 400, body)
        type(self).d_status = 200
        status, body = self.request()
        self.assertEqual(status, 200, body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
