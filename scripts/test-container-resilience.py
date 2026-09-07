#!/usr/bin/env python3
"""Exercise disposable brokers with measured delivery and real network faults."""
import argparse
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
import urllib.request

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--engine', default=shutil.which('docker') or shutil.which('podman'))
p.add_argument('--image', required=True, help='prebuilt broker image')
p.add_argument('--driver', default='bin/spruce-lifecycle')
p.add_argument('--brokers', type=int, choices=[1, 3, 5], default=3)
p.add_argument('--case', choices=['baseline', 'partition', 'kill-two'], default='baseline')
p.add_argument('--messages', type=int, default=2000, help='per producer')
p.add_argument('--rate', type=float, default=200)
p.add_argument('--handler-concurrency', type=int, default=1)
p.add_argument('--groups', type=int, default=2)
p.add_argument('--fault-after', type=float, default=10)
p.add_argument('--fault-seconds', type=float, default=20)
p.add_argument('--timeout', type=int, default=150)
p.add_argument('--retention-seconds', type=int, default=600)
p.add_argument('--resources', action='store_true', help='sample Linux cgroup CPU and memory')
a = p.parse_args()
if not 1 <= a.handler_concurrency <= 32 or not 1 <= a.retention_seconds <= 86400 or not a.engine or not 1 <= a.messages <= 250000 or not math.isfinite(a.rate) or a.rate < 0 or not 1 <= a.groups <= 16 or not 0 < a.timeout <= 1800 or not math.isfinite(a.fault_after) or not math.isfinite(a.fault_seconds) or a.fault_after < 0 or a.fault_seconds <= 0:
    p.error('invalid scenario bounds')
if a.case != 'baseline' and (a.brokers < 3 or a.rate == 0 or a.messages * 4 / a.rate < a.fault_after + a.fault_seconds + 5):
    p.error('fault cases need three brokers and traffic spanning failure and recovery')
root = Path(__file__).resolve().parents[1]
prefix = f'spruce-proof-{os.getpid()}'
network = prefix
names = [f'{prefix}-broker{i}' for i in range(1, a.brokers + 1)]
gateway = f'{prefix}-gateway'
created = []
process = None
sampler = None

def engine(*args, check=True):
    return subprocess.run([a.engine, *args], check=check, capture_output=True, text=True, timeout=60).stdout.strip()

def event(**data):
    print(json.dumps(data), flush=True)

def status(i):
    return json.loads(engine('exec', gateway, 'wget', '-qO-', f'http://broker{i}:8080/v1/status'))

with tempfile.TemporaryDirectory(prefix=prefix) as temporary:
    folder = Path(temporary)
    try:
        engine('network', 'create', network)
        for i, name in enumerate(names, 1):
            peers = ','.join(f'http://broker{j}:8080' for j in range(1, a.brokers + 1) if j != i)
            settings = {'SPRUCE_PEERS': peers, 'SPRUCE_PEER_TOKEN': 'synthetic-container-proof',
                        'SPRUCE_CLUSTER_ID': prefix, 'SPRUCE_ALLOW_ANONYMOUS': 'true',
                        'SPRUCE_ALLOW_INSECURE_TRANSPORT': 'true', 'SPRUCE_DEFAULT_TTL': f'{a.retention_seconds}s',
                        'SPRUCE_CACHE_BYTES': str(32 << 20), 'SPRUCE_REPLICATION_QUEUE_BYTES': str(8 << 20),
                        'SPRUCE_ACK_DEADLINE': '5s', 'SPRUCE_DRAIN_DELAY': '1s'}
            env = [item for key, value in settings.items() for item in ('-e', f'{key}={value}')]
            engine('run', '-d', '--name', name, '--network', network, '--network-alias', f'broker{i}',
                   '--memory', '256m', '--read-only', '--cap-drop', 'ALL', *env, a.image)
            created.append(name)
        config = (root / 'deploy/nginx.conf').read_text()
        servers = ' '.join(f'server broker{i}:8080 resolve fail_timeout=1s;' for i in range(1, a.brokers + 1))
        config = re.sub(r'server broker1:8080 resolve fail_timeout=1s; server broker2:8080 resolve fail_timeout=1s; server broker3:8080 resolve fail_timeout=1s;', servers, config)
        (folder / 'nginx.conf').write_text(config)
        engine('run', '-d', '--name', gateway, '--network', network, '-p', '127.0.0.1::8080',
               '-v', f'{folder}/nginx.conf:/spruce-nginx.conf.template:ro,Z',
               '-v', f'{root}/deploy/nginx-entrypoint.sh:/spruce-nginx-entrypoint.sh:ro,z',
               '--entrypoint', '/bin/sh', 'nginx:1.29-alpine', '/spruce-nginx-entrypoint.sh')
        created.append(gateway)
        port = engine('port', gateway, '8080/tcp').splitlines()[0].rsplit(':', 1)[1]
        base = f'http://127.0.0.1:{port}'
        deadline = time.monotonic() + 40
        while True:
            try:
                with urllib.request.urlopen(base + '/health/ready', timeout=2): break
            except OSError:
                if time.monotonic() >= deadline: raise RuntimeError('brokers never became ready')
                time.sleep(.2)
        # A healthy comparison begins only after every broker completes bootstrap.
        for i in range(1, a.brokers + 1):
            while True:
                try:
                    engine('exec', gateway, 'wget', '-qO-', f'http://broker{i}:8080/health/ready')
                    break
                except subprocess.CalledProcessError:
                    if time.monotonic() >= deadline: raise RuntimeError('broker bootstrap did not complete')
                    time.sleep(.2)
        if a.resources:
            resource_output = (folder / 'resources.jsonl').open('w')
            sampler = subprocess.Popen(['python3', str(root / 'scripts/sample-container-resources.py'),
                                        '--engine', a.engine, '--seconds', str(a.timeout + 60),
                                        *names, gateway], stdout=resource_output)
            resource_output.close()
        args = [str(root / a.driver), '-server', base, '-messages', str(a.messages), '-rate', str(a.rate),
                '-groups', str(a.groups), '-handler-concurrency', str(a.handler_concurrency), '-ack', 'available', '-timeout', f'{a.timeout}s', '-ttl', f'{a.retention_seconds}s', '-allow-duplicates']
        if a.case == 'partition': args.append('-allow-reorder')
        with (folder / 'driver.log').open('w+') as output:
            process = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT, cwd=root)
            ready_at = None
            faulted = healed = False
            deadline = time.monotonic() + a.timeout + 15
            while process.poll() is None:
                output.flush()
                contents = (folder / 'driver.log').read_text()
                if ready_at is None and 'subscriptions_ready=true' in contents:
                    ready_at = time.monotonic()
                elapsed = time.monotonic() - ready_at if ready_at is not None else 0
                if a.case != 'baseline' and ready_at is not None and not faulted and elapsed >= a.fault_after:
                    targets = [names[-1]] if a.case == 'partition' else names[-2:]
                    for name in targets:
                        if a.case == 'partition':
                            engine('network', 'disconnect', network, name)
                        else: engine('kill', '--signal', 'KILL', name)
                    faulted = True
                    event(fault=a.case, elapsed=elapsed, targets=len(targets))
                if faulted and not healed and elapsed >= a.fault_after + a.fault_seconds:
                    for name in targets:
                        if a.case == 'partition':
                            index = names.index(name) + 1
                            engine('network', 'connect', '--alias', f'broker{index}', network, name)
                        else: engine('start', name)
                    healed = True
                    event(healed=True, elapsed=elapsed)
                if time.monotonic() > deadline: raise RuntimeError('driver deadline exceeded')
                time.sleep(.2)
            contents = (folder / 'driver.log').read_text()
            print(contents, end='', flush=True)
            if process.returncode: raise RuntimeError(f'delivery oracle failed: {process.returncode}')
            if a.case != 'baseline' and not healed: raise RuntimeError('driver finished before fault cycle completed')
        report = next(json.loads(line) for line in contents.splitlines() if line.startswith('{'))
        deadline = time.monotonic() + 45
        while True:
            states = [status(i) for i in range(1, a.brokers + 1)]
            digests = [engine('exec', gateway, 'wget', '-qO-', '--header=Spruce-Peer-Token: synthetic-container-proof',
                              f'--header=Spruce-Cluster-ID: {prefix}', f'http://broker{i}:8080/internal/cache-digest')
                       for i in range(1, a.brokers + 1)]
            within_retention = time.monotonic() - ready_at < a.retention_seconds
            counts_match = all(s['messages'] == states[0]['messages'] for s in states)
            if (counts_match and (not within_retention or states[0]['messages'] == report['accepted'])
                    and len(set(digests)) == 1 and all(s.get('repair_pending_peers', 0) == 0 for s in states)): break
            if time.monotonic() >= deadline: raise RuntimeError(f'replicas did not converge: {states}')
            time.sleep(1)
        event(converged_messages=[s['messages'] for s in states], cache_digest=digests[0],
              repair_pages=[s.get('repair_pages', 0) for s in states],
              pending_deliveries=[s['pending_deliveries'] for s in states])
        if sampler is not None:
            sampler.terminate()
            sampler.wait(timeout=5)
            observations = {}
            errors = 0
            for line in (folder / 'resources.jsonl').read_text().splitlines():
                for sample in json.loads(line)['samples']:
                    if 'error' in sample:
                        errors += 1
                        continue
                    observations.setdefault(sample['container'], []).append(sample)
            summary = []
            for name, samples in observations.items():
                cpu = [s['cpu_cores'] for s in samples if s['cpu_cores'] is not None]
                memory = sorted(s['memory_bytes'] for s in samples)
                summary.append({'component': 'gateway' if name == gateway else f'broker{names.index(name) + 1}',
                                'samples': len(samples), 'mean_cpu_cores': sum(cpu) / len(cpu) if cpu else None,
                                'p95_memory_bytes': memory[min(len(memory) - 1, int(len(memory) * .95))],
                                'peak_memory_bytes': max(s['cgroup_peak_bytes'] for s in samples)})
            event(resources=summary, resource_sample_errors=errors)
    finally:
        if sampler is not None and sampler.poll() is None:
            sampler.terminate()
            sampler.wait(timeout=5)
        if process is not None and process.poll() is None:
            process.terminate()
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
        for args in [('rm', '-f', name) for name in reversed(created)] + [('network', 'rm', network)]:
            try:
                engine(*args, check=False)
            except (OSError, subprocess.TimeoutExpired) as exc:
                event(cleanup_error=str(exc))
