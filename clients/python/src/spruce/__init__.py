from __future__ import annotations

import base64
import json
import queue
import random
import ssl
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Callable, Iterator, Mapping, Sequence

MAX_MESSAGE_BYTES = 1 << 20
MAX_BATCH_BYTES = 16 << 20
MAX_BATCH_MESSAGES = 4096
__version__ = "0.1.0"
_DEFAULT_TIMEOUT = object()


class SpruceError(Exception):
    def __init__(self, status_code: int, status: str, code: str = "", body: str = "") -> None:
        super().__init__(f"Spruce {status_code} {status}: {code or body}")
        self.status_code, self.status, self.code, self.body = status_code, status, code, body


class HandlerPanicError(RuntimeError):
    pass


@dataclass(frozen=True)
class ClientEvent:
    operation: str
    duration: float
    status_code: int | None
    error: BaseException | None


@dataclass(frozen=True)
class PublishOptions:
    key: str = ""
    content_type: str = ""
    producer_id: str = ""
    idempotency_key: str = ""
    ack: str = ""
    ttl: str = ""


@dataclass(frozen=True)
class PublishResult:
    id: str
    replicated: bool = False


@dataclass(frozen=True)
class Delivery:
    delivery_id: str
    message_id: str
    topic: str
    payload: bytes
    key: str = ""
    headers: Mapping[str, str] = field(default_factory=dict)
    created_at: int = 0
    attempt: int = 0


@dataclass(frozen=True)
class SubscribeOptions:
    topic: str
    group: str = ""
    since: int = 0
    concurrency: int = 16
    max_payload_bytes: int = MAX_MESSAGE_BYTES
    drain_timeout: float = 1.0


@dataclass(frozen=True)
class RetryOptions:
    max_attempts: int = 3
    min_backoff: float = 0.05
    max_backoff: float = 2.0


@dataclass(frozen=True)
class BrokerStatus:
    messages: int
    cache_accounted_bytes: int
    cache_limit_bytes: int
    peers: int
    consumers: int
    pending_deliveries: int


class _NoDowngrade(urllib.request.HTTPRedirectHandler):
    def __init__(self, allow_insecure: bool) -> None:
        self.allow_insecure = allow_insecure

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if req.has_header("Authorization") and urllib.parse.urlsplit(newurl).scheme != "https" and not self.allow_insecure:
            raise urllib.error.URLError("refusing credential redirect to plaintext HTTP")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class Client:
    def __init__(self, base_url: str, *, token: str = "", username: str = "", password: str = "", allow_insecure_credentials: bool = False, timeout: float = 30.0, ssl_context: ssl.SSLContext | None = None, on_event: Callable[[ClientEvent], None] | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.token, self.username, self.password = token, username, password
        self.allow_insecure_credentials, self.timeout, self.on_event = allow_insecure_credentials, timeout, on_event
        scheme = urllib.parse.urlsplit(self.base_url).scheme
        if (token or username) and scheme != "https" and not allow_insecure_credentials:
            raise ValueError("credentials require HTTPS; allow insecure credentials only for isolated development")
        handlers = [_NoDowngrade(allow_insecure_credentials)]
        if ssl_context is not None: handlers.append(urllib.request.HTTPSHandler(context=ssl_context))
        self._opener = urllib.request.build_opener(*handlers)

    def _headers(self) -> dict[str, str]:
        if self.token:
            return {"Authorization": f"Bearer {self.token}"}
        if self.username:
            encoded = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            return {"Authorization": f"Basic {encoded}"}
        return {}

    def _emit(self, operation: str, started: float, status: int | None, error: BaseException | None) -> None:
        if self.on_event:
            try:
                self.on_event(ClientEvent(operation, time.monotonic() - started, status, error))
            except BaseException:
                pass

    def _request(self, method: str, path: str, data: bytes | None = None, headers: Mapping[str, str] | None = None, *, timeout: float | None | object = _DEFAULT_TIMEOUT, operation: str = "request"):
        started, status = time.monotonic(), None
        merged = self._headers(); merged.update(headers or {})
        request = urllib.request.Request(self.base_url + path, data=data, headers=merged, method=method)
        try:
            response = self._opener.open(request, timeout=self.timeout if timeout is _DEFAULT_TIMEOUT else timeout)
            status = response.status
            self._emit(operation, started, status, None)
            return response
        except urllib.error.HTTPError as exc:
            status = exc.code
            raw = exc.read(4096).decode("utf-8", "replace")
            try: code = json.loads(raw).get("error", "")
            except json.JSONDecodeError: code = ""
            error = SpruceError(exc.code, exc.reason, code, raw.strip())
            self._emit(operation, started, status, error)
            raise error from exc
        except BaseException as exc:
            self._emit(operation, started, status, exc)
            raise

    @staticmethod
    def _option_headers(options: PublishOptions) -> dict[str, str]:
        values = {"Spruce-Key": options.key, "Content-Type": options.content_type, "Spruce-Producer-ID": options.producer_id, "Spruce-Idempotency-Key": options.idempotency_key, "Spruce-Ack": options.ack, "Spruce-TTL": options.ttl}
        return {key: value for key, value in values.items() if value}

    def publish(self, topic: str, payload: bytes, options: PublishOptions = PublishOptions()) -> PublishResult:
        if not topic: raise ValueError("topic is required")
        if len(payload) > MAX_MESSAGE_BYTES: raise ValueError("payload exceeds 1 MiB")
        path = f"/v1/topics/{urllib.parse.quote(topic, safe='')}/messages"
        with self._request("POST", path, bytes(payload), self._option_headers(options), operation="publish") as response:
            value = json.load(response)
        return PublishResult(value["id"], bool(value.get("replicated", False)))

    def publish_with_retry(self, topic: str, payload: bytes, options: PublishOptions, retry: RetryOptions = RetryOptions()) -> PublishResult:
        if not options.producer_id or not options.idempotency_key: raise ValueError("retry requires producer ID and idempotency key")
        if retry.max_attempts < 1 or retry.min_backoff < 0 or retry.max_backoff < retry.min_backoff: raise ValueError("invalid retry options")
        delay = retry.min_backoff
        for attempt in range(retry.max_attempts):
            try: return self.publish(topic, payload, options)
            except SpruceError as exc:
                if exc.status_code not in (408, 429, 503) or attempt + 1 == retry.max_attempts: raise
            except (OSError, urllib.error.URLError):
                if attempt + 1 == retry.max_attempts: raise
            time.sleep(delay); delay = min(retry.max_backoff, delay * 2)
        raise RuntimeError("unreachable")

    def publish_batch(self, topic: str, payloads: Sequence[bytes], options: PublishOptions = PublishOptions()) -> list[PublishResult]:
        if not payloads or len(payloads) > MAX_BATCH_MESSAGES: raise ValueError("batch message count is outside protocol limits")
        if options.idempotency_key or options.ack not in ("", "local"): raise ValueError("options are incompatible with batch publishing")
        total = sum(4 + len(item) for item in payloads)
        if total > MAX_BATCH_BYTES or any(len(item) > MAX_MESSAGE_BYTES for item in payloads): raise ValueError("batch exceeds protocol limits")
        body = b"".join(struct.pack(">I", len(item)) + bytes(item) for item in payloads)
        path = f"/v1/topics/{urllib.parse.quote(topic, safe='')}/batches"
        with self._request("POST", path, body, self._option_headers(options), operation="publish_batch") as response:
            ids = json.load(response).get("ids", [])
        if len(ids) != len(payloads): raise ValueError("Spruce returned an invalid batch result count")
        return [PublishResult(item) for item in ids]

    def _get(self, path: str, *, operation: str = "get") -> bytes:
        with self._request("GET", path, operation=operation) as response: return response.read()

    def status(self) -> BrokerStatus:
        return BrokerStatus(**json.loads(self._get("/v1/status")))

    def check_health(self) -> None:
        self._get("/health/ready")

    def is_message_cached(self, message_id: str) -> bool:
        try: self._get(f"/v1/status/messages/{urllib.parse.quote(message_id, safe='')}"); return True
        except SpruceError as exc:
            if exc.status_code == 404: return False
            raise

    def metrics(self) -> str:
        return self._get("/metrics", operation="metrics").decode()

    def _ack(self, action: str, ids: Sequence[str]) -> None:
        body = json.dumps({"delivery_ids": list(ids)}, separators=(",", ":")).encode()
        with self._request("POST", f"/v1/deliveries/{action}", body, {"Content-Type": "application/json"}, timeout=10, operation=action): pass

    @staticmethod
    def _read_exact(stream, length: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < length:
            chunk = stream.read(length - len(chunks))
            if not chunk: raise EOFError
            chunks.extend(chunk)
        return bytes(chunks)

    def _read_delivery(self, stream, maximum: int) -> Delivery:
        metadata_length, payload_length = struct.unpack(">II", self._read_exact(stream, 8))
        if metadata_length > 65536 or payload_length > maximum: raise ValueError("invalid Spruce frame size")
        metadata = json.loads(self._read_exact(stream, metadata_length))
        payload = self._read_exact(stream, payload_length)
        return Delivery(metadata.get("delivery_id", ""), metadata.get("message_id", ""), metadata.get("topic", ""), payload, metadata.get("key", ""), metadata.get("headers", {}), metadata.get("created_at", 0), metadata.get("attempt", 0))

    def subscribe(self, options: SubscribeOptions, handler: Callable[[Delivery], object], stop: threading.Event | None = None) -> None:
        if not options.topic: raise ValueError("topic is required")
        if not 1 <= options.concurrency <= 1024 or not 1 <= options.max_payload_bytes <= 64 << 20: raise ValueError("invalid subscription limits")
        stop = stop or threading.Event(); since, backoff = options.since, 0.05
        acks, nacks = _AckBatcher(self, "ack"), _AckBatcher(self, "nack")
        try:
          while not stop.is_set():
            query = {"topic": options.topic};
            if options.group: query["group"] = options.group
            if since: query["since"] = str(since)
            futures: list[tuple[int, object]] = []; sequence, next_progress, completed = 0, 1, {}
            def advance() -> None:
                nonlocal since, next_progress
                while next_progress in completed:
                    since = max(since, completed.pop(next_progress)); next_progress += 1
            try:
                response = self._request("GET", "/v1/subscriptions/stream?" + urllib.parse.urlencode(query), timeout=None, operation="subscribe")
                def interrupt() -> None:
                    stop.wait()
                    if stop.is_set(): response.close()
                threading.Thread(target=interrupt, daemon=True).start()
                with response, ThreadPoolExecutor(max_workers=options.concurrency) as workers:
                    while not stop.is_set():
                        delivery = self._read_delivery(response, options.max_payload_bytes)
                        if not delivery.delivery_id: continue
                        sequence += 1
                        def consume(item=delivery):
                            try: handler(item)
                            except Exception:
                                nacks.submit(item.delivery_id); return item.created_at
                            except BaseException as exc:
                                nacks.submit(item.delivery_id); raise HandlerPanicError(str(exc)) from exc
                            acks.submit(item.delivery_id); return item.created_at
                        futures.append((sequence, workers.submit(consume)))
                        if len(futures) >= options.concurrency * 4:
                            done, _ = wait([future for _, future in futures], timeout=options.drain_timeout)
                            pending = []
                            for index, future in futures:
                                if future in done: completed[index] = future.result()
                                else: pending.append((index, future))
                            futures = pending; advance()
            except SpruceError as exc:
                if 400 <= exc.status_code < 500 and exc.status_code not in (408, 429): raise
            except HandlerPanicError: raise
            except (EOFError, OSError, urllib.error.URLError): pass
            for index, future in futures:
                try: completed[index] = future.result(timeout=options.drain_timeout); advance()
                except TimeoutError as exc: raise TimeoutError("Spruce handlers did not stop before drain timeout") from exc
            if stop.wait(random.uniform(backoff / 2, backoff * 1.5)): return
            backoff = min(2.0, backoff * 2)
        finally:
            acks.close(); nacks.close()

    def deliveries(self, options: SubscribeOptions, stop: threading.Event | None = None) -> Iterator["ConsumableDelivery"]:
        stop = stop or threading.Event(); items: queue.Queue[ConsumableDelivery | BaseException] = queue.Queue(maxsize=max(1, options.concurrency * 2))
        def handler(delivery: Delivery) -> None:
            item = ConsumableDelivery(delivery); items.put(item); error = item._completed.get()
            if error: raise error
        def run() -> None:
            try: self.subscribe(options, handler, stop)
            except BaseException as exc: items.put(exc)
        thread = threading.Thread(target=run, daemon=True); thread.start()
        try:
            while True:
                item = items.get()
                if isinstance(item, BaseException): raise item
                yield item
        finally:
            stop.set(); thread.join(options.drain_timeout)


class ConsumableDelivery:
    def __init__(self, delivery: Delivery) -> None:
        self.delivery, self._completed = delivery, queue.Queue(maxsize=1)
    def complete(self, error: BaseException | None = None) -> None:
        try: self._completed.put_nowait(error)
        except queue.Full: pass


class _AckBatcher:
    def __init__(self, client: Client, action: str) -> None:
        self.client, self.action, self.items = client, action, queue.Queue(1024)
        self.thread = threading.Thread(target=self._run, daemon=True); self.thread.start()
    def submit(self, delivery_id: str) -> None:
        result: queue.Queue = queue.Queue(1); self.items.put((delivery_id, result)); error = result.get()
        if error: raise error
    def close(self) -> None:
        self.items.put(None); self.thread.join(10)
    def _run(self) -> None:
        while True:
            first = self.items.get()
            if first is None: return
            batch = [first]; deadline = time.monotonic() + .0005
            while len(batch) < 256:
                try: item = self.items.get(timeout=max(0, deadline - time.monotonic()))
                except queue.Empty: break
                if item is None:
                    self.items.put(None); break
                batch.append(item)
            try: self.client._ack(self.action, [item[0] for item in batch]); error = None
            except BaseException as exc: error = exc
            for _, result in batch: result.put(error)


class Deduper:
    def __init__(self, max_entries: int = 65536, ttl: float = 300.0) -> None:
        if max_entries < 1 or ttl <= 0: raise ValueError("invalid deduper limits")
        self.max_entries, self.ttl, self._seen, self._order, self._lock = max_entries, ttl, {}, [], threading.Lock()
    def seen(self, message_id: str) -> bool:
        with self._lock:
            now = time.monotonic()
            if self._seen.get(message_id, 0) > now: return True
            until = now + self.ttl; self._seen[message_id] = until; self._order.append((message_id, until))
            while len(self._order) > self.max_entries:
                old, expiry = self._order.pop(0)
                if self._seen.get(old) == expiry: self._seen.pop(old, None)
            return False


@dataclass(frozen=True)
class BatcherOptions:
    max_messages: int = 256
    max_bytes: int = 1 << 20
    max_delay: float = 0.00025
    queue_depth: int = 4096


class ProducerBatcher:
    def __init__(self, client: Client, options: BatcherOptions = BatcherOptions()) -> None:
        if not 1 <= options.max_messages <= 4096 or not 5 <= options.max_bytes <= MAX_BATCH_BYTES or options.max_delay <= 0 or options.queue_depth < 1: raise ValueError("invalid batcher options")
        self.client, self.options, self._queue, self._closed = client, options, queue.Queue(options.queue_depth), False
        self._thread = threading.Thread(target=self._run, daemon=True); self._thread.start()
    def publish(self, topic: str, payload: bytes, options: PublishOptions = PublishOptions(), timeout: float | None = None) -> PublishResult:
        if self._closed: raise RuntimeError("producer batcher is closed")
        if not topic or len(payload) > MAX_MESSAGE_BYTES or len(payload) + 4 > self.options.max_bytes: raise ValueError("invalid batch publish")
        if options.idempotency_key or options.ack not in ("", "local"): raise ValueError("options are incompatible with batch publishing")
        result: queue.Queue = queue.Queue(1); self._queue.put((topic, bytes(payload), options, result), timeout=timeout); value = result.get(timeout=timeout)
        if isinstance(value, BaseException): raise value
        return value
    def flush(self, timeout: float | None = None) -> None:
        result: queue.Queue = queue.Queue(1); self._queue.put((None, None, None, result), timeout=timeout); value = result.get(timeout=timeout)
        if value: raise value
    def close(self, timeout: float | None = 30.0) -> None:
        if self._closed: return
        error = None
        try: self.flush(timeout)
        except BaseException as exc: error = exc
        self._closed = True; self._queue.put((False, None, None, None)); self._thread.join(timeout)
        if self._thread.is_alive(): raise TimeoutError("producer batcher did not stop")
        if error: raise error
    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): self.close()
    def _run(self) -> None:
        pending, deadline = [], None
        def flush():
            nonlocal pending, deadline
            if not pending: return None
            batch, pending, deadline = pending, [], None
            try:
                values = self.client.publish_batch(batch[0][0], [item[1] for item in batch], batch[0][2])
                for item, value in zip(batch, values): item[3].put(value)
                return None
            except BaseException as exc:
                for item in batch: item[3].put(exc)
                return exc
        while True:
            delay = None if deadline is None else max(0, deadline - time.monotonic())
            try: item = self._queue.get(timeout=delay)
            except queue.Empty: flush(); continue
            if item[0] is False: flush(); return
            if item[0] is None: item[3].put(flush()); continue
            size = sum(4 + len(value[1]) for value in pending)
            compatible = not pending or (pending[0][0] == item[0] and pending[0][2] == item[2])
            if pending and (not compatible or len(pending) >= self.options.max_messages or size + 4 + len(item[1]) > self.options.max_bytes): flush()
            pending.append(item)
            if len(pending) == 1: deadline = time.monotonic() + self.options.max_delay
            if len(pending) >= self.options.max_messages or sum(4 + len(value[1]) for value in pending) >= self.options.max_bytes: flush()


__all__ = ["BatcherOptions", "BrokerStatus", "Client", "ClientEvent", "ConsumableDelivery", "Deduper", "Delivery", "HandlerPanicError", "ProducerBatcher", "PublishOptions", "PublishResult", "RetryOptions", "SpruceError", "SubscribeOptions", "__version__"]
