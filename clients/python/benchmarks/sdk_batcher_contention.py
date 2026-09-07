"""Measure Python SDK batcher admission contention without a broker.

Each source is loaded from a git revision or an explicit ``__init__.py`` path.
The stub client acknowledges every entry, so the reported rate is completed SDK
publishes per second, not broker throughput.
"""

import argparse
import gc
import json
import os
import pathlib
import platform
import statistics
import subprocess
import sys
import threading
import time
import types


def _source(repo: str, value: str) -> tuple[str, str]:
    path = pathlib.Path(value)
    if path.is_file():
        return str(path), path.read_text()
    relative = "clients/python/src/spruce/__init__.py"
    data = subprocess.check_output(["git", "-C", repo, "show", f"{value}:{relative}"])
    return value, data.decode()


def _module(repo: str, value: str, index: int):
    label, source = _source(repo, value)
    name = f"spruce_bench_{index}_{abs(hash(label))}"
    module = types.ModuleType(name)
    sys.modules[name] = module
    exec(compile(source, label, "exec"), module.__dict__)
    return label, module


def _trial(module, workers: int, per_worker: int, options) -> dict:
    class Stub:
        def publish_batch_entries(self, topic, entries, publish_options):
            return [module.PublishResult(f"id-{index}") for index, _ in enumerate(entries)]

    batcher = module.ProducerBatcher(Stub(), options)
    start = threading.Event()
    failures = []
    failure_lock = threading.Lock()

    def run():
        start.wait()
        try:
            for _ in range(per_worker):
                batcher.publish("benchmark", b"x")
        except BaseException as exc:
            with failure_lock:
                failures.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=run) for _ in range(workers)]
    for thread in threads:
        thread.start()
    started = time.perf_counter()
    start.set()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started
    try:
        batcher.close()
    except BaseException as exc:
        with failure_lock:
            failures.append(f"close {type(exc).__name__}: {exc}")
    completed = workers * per_worker - len(failures)
    return {"elapsed_seconds": elapsed, "completed_messages": completed, "errors": failures, "messages_per_second": completed / elapsed if elapsed else 0.0}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=os.getcwd())
    parser.add_argument("--revisions", nargs=2, default=["25d8233", "HEAD"])
    parser.add_argument("--workers", type=int, nargs="+", default=[128])
    parser.add_argument("--queue-depth", type=int, default=8)
    parser.add_argument("--max-messages", type=int, default=256)
    parser.add_argument("--per-worker", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    options_by_module = []
    loaded = []
    for index, revision in enumerate(args.revisions):
        label, module = _module(args.repo, revision, index)
        loaded.append((label, module))
        options_by_module.append(module.BatcherOptions(max_messages=args.max_messages, queue_depth=args.queue_depth, max_delay=.00025))

    print(json.dumps({"python": platform.python_version(), "platform": platform.platform(), "settings": vars(args), "payload_bytes": 1, "max_delay_seconds": .00025}), flush=True)
    for workers in args.workers:
        rows = []
        schedule = [(repeat, index) for repeat in range(args.repeats) for index in range(len(loaded))]
        for repeat, index in schedule:
            label, module = loaded[index]
            result = _trial(module, workers, args.per_worker, options_by_module[index])
            row = {"revision": label, "workers": workers, "repeat": repeat, **result}
            rows.append(row)
            print(json.dumps(row), flush=True)
            if result["errors"]:
                return 1
        for index, (label, _) in enumerate(loaded):
            rates = [row["messages_per_second"] for row in rows if row["revision"] == label]
            print(json.dumps({"revision": label, "workers": workers, "median_messages_per_second": statistics.median(rates), "raw_messages_per_second": rates}), flush=True)
    gc.collect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
