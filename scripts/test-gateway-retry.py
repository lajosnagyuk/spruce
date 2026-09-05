#!/usr/bin/env python3
"""Verify real nginx retries only operation-identified single-message publishes."""
import argparse
import http.server
from pathlib import Path
import shutil
import socket
import re
import textwrap
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--engine', default=shutil.which('docker') or shutil.which('podman'))
parser.add_argument('--helm', action='store_true', help='test the rendered Helm gateway configuration')
parser.add_argument('--image', default='nginx:1.29-alpine')
args = parser.parse_args()
if not args.engine: parser.error('Docker or Podman is required')
lock = threading.Lock()
requests = []
payload = b'\x00\xffopaque\x10\xfd'
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        with lock:
            requests.append((self.path, body, self.headers.get('Spruce-Producer-ID'), self.headers.get('Spruce-Idempotency-Key')))
            status = 503 if len(requests) == 1 else 202
        self.send_response(status)
        self.send_header('Content-Length', '0')
        self.end_headers()

servers = [http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler) for _ in range(3)]
for server in servers:
    threading.Thread(target=server.serve_forever, daemon=True).start()
with socket.socket() as probe:
    probe.bind(('127.0.0.1', 0)); port = probe.getsockname()[1]
name = f'spruce-gateway-retry-{time.time_ns()}'
try:
    with tempfile.TemporaryDirectory(prefix='spruce-gateway-') as directory:
        root = Path(__file__).resolve().parents[1]
        if args.helm:
            rendered = subprocess.check_output(['helm', 'template', 'spruce', str(root / 'deploy/helm/spruce'),
                '--namespace', 'default', '--show-only', 'templates/gateway.yaml',
                '--set', 'image.tag=dev', '--set', 'image.pullPolicy=Never', '--set', 'tls.allowInsecureTransport=true',
                '--set', 'gateway.allowInsecureClientTransport=true'], text=True)
            config = textwrap.dedent(rendered.split('nginx.conf: |\n', 1)[1].split('\n---', 1)[0])
        else:
            config = (root / 'deploy/nginx.conf').read_text()
        config = re.sub(r'(?m)^  resolver .*$', '  resolver 127.0.0.1 valid=1s ipv6=off;', config)
        config = config.replace('${SPRUCE_DNS_RESOLVER}', '127.0.0.1').replace(' resolve;', ';').replace(' resolve fail_timeout', ' fail_timeout')
        config = config.replace('listen 8080;', f'listen 127.0.0.1:{port};')
        for i, server in enumerate(servers):
            config = config.replace(f'broker{i+1}:8080', f'127.0.0.1:{server.server_port}')
            config = config.replace(f'spruce-{i}.spruce-headless.default.svc.cluster.local:8080', f'127.0.0.1:{server.server_port}')
        path = Path(directory) / 'nginx.conf'; path.write_text(config)
        subprocess.run([args.engine, 'run', '-d', '--name', name, '--network', 'host',
                        '-v', f'{path}:/etc/nginx/nginx.conf:ro,Z', args.image],
                       check=True, stdout=subprocess.DEVNULL)
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2): break
            except OSError:
                if time.monotonic() >= deadline:
                    subprocess.run([args.engine, 'logs', name], check=False)
                    raise RuntimeError('nginx did not start')
                time.sleep(.05)
        for index, (suffix, headers, expected) in enumerate([
            ('messages', {'Spruce-Producer-ID':'p','Spruce-Idempotency-Key':'operation'}, 2),
            ('messages', {}, 1),
            ('messages', {'Spruce-Producer-ID':'p'}, 1),
            ('messages', {'Spruce-Idempotency-Key':'operation'}, 1),
            ('batches', {'Spruce-Producer-ID':'p','Spruce-Idempotency-Key':'operation'}, 1),
        ]):
            with lock: requests.clear()
            route = f'/v1/topics/test-{index}/{suffix}?ack=available'
            request = urllib.request.Request(f'http://127.0.0.1:{port}{route}', data=payload, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=5) as response: status = response.status
            except urllib.error.HTTPError as error: status = error.code; error.close()
            with lock: observed = list(requests)
            assert len(observed) == expected, (suffix, headers, status, observed)
            assert status == (202 if expected == 2 else 503), status
            assert all(row == (route, payload, headers.get('Spruce-Producer-ID'), headers.get('Spruce-Idempotency-Key')) for row in observed), observed
        print('gateway retry passed: identified single publish retries unchanged; plain and batch POSTs do not')
finally:
    subprocess.run([args.engine, 'rm', '-f', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for server in servers: server.shutdown(); server.server_close()
