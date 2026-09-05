#!/usr/bin/env python3
"""Read cgroup-v2 CPU and memory for named local test containers as JSON lines."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('containers', nargs='+')
parser.add_argument('--engine', default=shutil.which('docker') or shutil.which('podman'))
parser.add_argument('--seconds', type=float, default=60)
parser.add_argument('--interval', type=float, default=1)
args = parser.parse_args()
if not args.engine or not 0 < args.seconds <= 86400 or not .1 <= args.interval <= args.seconds:
    parser.error('need a container engine, bounded duration, and interval >= 0.1s')

locations = {}
previous = {}
end = time.monotonic() + args.seconds
while time.monotonic() < end:
    started = time.monotonic()
    samples = []
    for name in args.containers:
        try:
            path = locations.get(name)
            if path is None or not (path / 'cpu.stat').exists():
                state = json.loads(subprocess.check_output([args.engine, 'inspect', name], text=True))[0]
                pid = state['State']['Pid']
                if not pid:
                    raise RuntimeError('container is stopped')
                relative = next(line.split('::', 1)[1] for line in Path(f'/proc/{pid}/cgroup').read_text().splitlines() if line.startswith('0::'))
                path = Path('/sys/fs/cgroup') / relative.lstrip('/')
                locations[name] = path
                previous.pop(name, None)
            cpu = dict(line.split() for line in (path / 'cpu.stat').read_text().splitlines())
            usage = int(cpu['usage_usec'])
            sampled = time.monotonic()
            old = previous.get(name)
            cores = (usage - old[1]) / 1e6 / (sampled - old[0]) if old else None
            previous[name] = (sampled, usage)
            samples.append({'container': name, 'cpu_cores': cores, 'memory_bytes': int((path / 'memory.current').read_text()), 'cgroup_peak_bytes': int((path / 'memory.peak').read_text())})
        except (OSError, RuntimeError, subprocess.CalledProcessError, KeyError, StopIteration) as exc:
            locations.pop(name, None)
            previous.pop(name, None)
            samples.append({'container': name, 'error': str(exc)})
    print(json.dumps({'time_unix': time.time(), 'samples': samples}), flush=True)
    time.sleep(max(0, min(args.interval - (time.monotonic() - started), end - time.monotonic())))
