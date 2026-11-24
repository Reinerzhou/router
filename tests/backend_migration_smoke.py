"""CPU smoke test of the actual binary's backend-specific HTTP wire contract."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def run(binary, backend, override, pd=False, raw_only=False):
    captured = []
    held = threading.Event()
    release = threading.Event()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    class Worker(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, body, content_type='application/json'):
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('X-Migration-Test', 'preserved')
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == '/distserve/engine_info':
                self.reply(200, b'{"tp_size":1}')
            else:
                self.reply(200, b'{"data":[{"id":"test-model"}]}')

        def do_POST(self):
            raw = self.rfile.read(int(self.headers.get('Content-Length', 0)))
            body = json.loads(raw)
            if self.path.startswith('/distserve/'):
                self.reply(200, b'{"status":1,"engine_endpoint_info":{},"kvtransfer_endpoint_info":[]}')
                return
            if pd and body.get('with_cache'):
                assert body['max_tokens'] == 1 and body['preserve_cache']
                self.reply(200, b'{"id":"42","cache_block_ids":[10,11],"remote_token_ids":[20,21]}')
                return
            if pd:
                migration = body['migration_request']
                assert migration['remote_session_id'] == 42
                assert migration['remote_block_ids'] == [10, 11]
                assert migration['remote_token_id'] == 21
                assert 'with_cache' not in body and 'preserve_cache' not in body
            captured.append((raw, body))
            if body.get('messages', [{}])[-1].get('content') == 'hold':
                held.set()
                if not release.wait(15):
                    self.reply(500, b'{"error":{"message":"hold timeout"}}')
                    return
            if body.get('stream'):
                self.reply(503, b'{"error":{"message":"mock unavailable"}}')
            else:
                self.reply(200, b'{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}')

    worker = ThreadingHTTPServer(('127.0.0.1', 0), Worker)
    threading.Thread(target=worker.serve_forever, daemon=True).start()
    router_port = port()
    metrics_port = port()
    decode = None
    args = [str(binary), '--port', str(router_port), '--prometheus-port', str(metrics_port),
            '--policy', 'round_robin' if pd else ('kv_aware' if raw_only else 'sticky_least_loaded'), '--disable-retries',
            '--disable-circuit-breaker', '--worker-startup-check-interval', '1']
    if pd:
        decode = ThreadingHTTPServer(('127.0.0.1', 0), Worker)
        threading.Thread(target=decode.serve_forever, daemon=True).start()
        args += ['--lmdeploy-pd-disaggregation', '--prefill',
                 f'http://127.0.0.1:{worker.server_port}', '--decode',
                 f'http://127.0.0.1:{decode.server_port}']
    else:
        args += ['--worker-urls', f'http://127.0.0.1:{worker.server_port}']
    if override:
        args += ['--backend', override]
    env = dict(os.environ, NO_PROXY='127.0.0.1,localhost', no_proxy='127.0.0.1,localhost')
    env.pop('OTEL_EXPORTER_OTLP_ENDPOINT', None)
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, env=env)
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise AssertionError('router exited during startup')
                try:
                    with opener.open(f'http://127.0.0.1:{router_port}/health', timeout=1):
                        break
                except OSError:
                    time.sleep(.05)
            else:
                raise AssertionError('startup timeout')

            def request(path, body, mime='application/json'):
                raw = body if isinstance(body, bytes) else json.dumps(body).encode()
                req = urllib.request.Request(f'http://127.0.0.1:{router_port}{path}', data=raw,
                                             headers={'Content-Type': mime, 'X-Session-ID': 'migration-session'})
                try:
                    response = opener.open(req, timeout=15)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    return response.status, response.headers, response.read()

            if raw_only:
                assert not pd
                raw = b' { "model":"test-model", "prompt":"hello", "extension":{"a":1} } '
                status, _, payload = request('/v1/completions', raw)
                assert status == 200, (status, payload)
                assert captured[-1][0] == raw, captured[-1][0]
                print(f'PASS {binary.name}: backend={backend}, KV-aware raw completion')
                return

            for reasoning, alias in [('canonical', 'legacy'), ('canonical', '')]:
                body = {'model': 'test-model', 'reasoning_effort': 'max', 'messages': [
                    {'role': 'developer', 'content': 'rules'},
                    {'role': 'assistant', 'content': 'answer', 'reasoning': reasoning,
                     'reasoning_content': alias},
                    {'role': 'user', 'content': [{'type': 'text', 'text': 'next'},
                                                {'type': 'custom_block', 'custom': {'a': 1}}]}]}
                status, _, payload = request('/v1/chat/completions', body)
                assert status == 200, (status, payload)
                forwarded = captured[-1][1]
                assert forwarded['messages'][0]['role'] == 'developer', forwarded
                assert forwarded['messages'][2]['content'] == body['messages'][2]['content'], forwarded
                assistant = forwarded['messages'][1]
                key = 'reasoning_content' if backend == 'lmdeploy' else 'reasoning'
                assert assistant[key] == (alias if backend == 'lmdeploy' else reasoning), assistant
                assert ('reasoning' if backend == 'lmdeploy' else 'reasoning_content') not in assistant
                assert forwarded['reasoning_effort'] == 'max'

            def running_load():
                with opener.open(f'http://127.0.0.1:{metrics_port}/metrics', timeout=5) as response:
                    lines = response.read().decode().splitlines()
                values = [float(line.rsplit(' ', 1)[1]) for line in lines
                          if line.startswith('vllm_router_running_requests{')]
                assert len(values) == 1, values
                return values[0]

            if backend == 'lmdeploy' and not pd:
                result = []
                thread = threading.Thread(target=lambda: result.append(request(
                    '/v1/chat/completions', {'model': 'test-model',
                    'messages': [{'role': 'user', 'content': 'hold'}]})))
                thread.start()
                try:
                    assert held.wait(10), 'request never reached worker'
                    assert running_load() == 1, 'request must be counted exactly once'
                finally:
                    release.set()
                    thread.join(15)
                assert result and result[0][0] == 200, result
                assert running_load() == 0, 'completed request must release its load'

            status, headers, payload = request('/v1/chat/completions', {
                'model': 'test-model', 'stream': True,
                'messages': [{'role': 'user', 'content': 'return mock error'}]})
            assert status == 503, (status, payload)
            assert 'application/json' in headers['Content-Type'], headers
            assert json.loads(payload)['error']['message'] == 'mock unavailable', payload
            assert headers['X-Migration-Test'] == 'preserved', headers
            if backend == 'lmdeploy' and not pd:
                assert running_load() == 0, 'failed streaming request leaked its load'

            # The upstream non-KV typed path preserves extension values, not JSON whitespace.
            raw = b' { "model":"test-model", "prompt":"hello", "extension":{"a":1} } '
            status, _, payload = request('/v1/completions', raw)
            assert status == 200, (status, payload)
            assert captured[-1][1]['extension'] == {'a': 1}, captured[-1][1]
            assert captured[-1][1]['prompt'] == 'hello', captured[-1][1]
            for body, mime in [(b'{', 'application/json'), (b'{}', 'application/json'),
                               (b'{}', 'text/plain')]:
                status, headers, payload = request('/v1/chat/completions', body, mime)
                assert status == 400, (status, payload)
                assert json.loads(payload)['error']['type'] == 'invalid_request_error', payload
            print(f'PASS {binary.name}: backend={backend}, override={override or "default"}, pd={pd}')
        except BaseException:
            log.seek(0)
            print(log.read().decode(errors='replace'))
            raise
        finally:
            process.terminate()
            process.wait(timeout=15)
            worker.shutdown()
            worker.server_close()
            if decode:
                decode.shutdown()
                decode.server_close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('binary', type=Path)
    parser.add_argument('--expected-backend', choices=['vllm', 'lmdeploy'], required=True)
    parser.add_argument('--backend-override', choices=['vllm', 'lmdeploy'])
    parser.add_argument('--lmdeploy-pd', action='store_true')
    parser.add_argument('--raw-completion-only', action='store_true')
    args = parser.parse_args()
    run(args.binary.resolve(), args.expected_backend, args.backend_override, args.lmdeploy_pd, args.raw_completion_only)
