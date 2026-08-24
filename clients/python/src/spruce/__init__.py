from __future__ import annotations

import base64
from collections import deque
import gzip
import io
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
import zstandard
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from email.utils import parsedate_to_datetime
from dataclasses import dataclass, field, replace
from typing import Callable, Iterator, Mapping, Sequence

MAX_MESSAGE_BYTES = 1 << 20
MAX_BATCH_BYTES = 16 << 20
MAX_BATCH_MESSAGES = 4096
__version__ = "0.3.0"
_DEFAULT_TIMEOUT = object()
_COMPRESSION_MAGIC = b"\x89SPRUCE\x01"
_compression_codecs = threading.local()


def _zstd_compressor():
    compressor = getattr(_compression_codecs, "compressor", None)
    if compressor is None:
        compressor = _compression_codecs.compressor = zstandard.ZstdCompressor(level=1, threads=0)
    return compressor


def _zstd_decompressor(maximum: int):
    state = getattr(_compression_codecs, "decompressor", None)
    if state is None or state[0] != maximum:
        state = _compression_codecs.decompressor = (maximum, zstandard.ZstdDecompressor(max_window_size=maximum))
    return state[1]


def _compress_payload(payload: bytes, algorithm: str) -> bytes:
    if not algorithm or len(payload) < 1024:
        return payload
    if algorithm not in ("gzip", "zstd"):
        raise ValueError(f"unsupported compression {algorithm!r}")
    if algorithm == "gzip":
        compressed = gzip.compress(payload, compresslevel=1, mtime=0)
        codec = b"\x01"
    else:
        compressed = _zstd_compressor().compress(payload)
        codec = b"\x02"
    encoded = _COMPRESSION_MAGIC + codec + struct.pack(">I", len(payload)) + compressed
    minimum_saving = max(128, len(payload) // 10)
    return encoded if len(encoded) <= len(payload) - minimum_saving else payload


def _decompress_payload(payload: bytes, maximum: int) -> bytes:
    if len(payload) < len(_COMPRESSION_MAGIC) + 5 or not payload.startswith(_COMPRESSION_MAGIC):
        return payload
    algorithm = payload[len(_COMPRESSION_MAGIC)]
    original = struct.unpack(">I", payload[len(_COMPRESSION_MAGIC) + 1:len(_COMPRESSION_MAGIC) + 5])[0]
    if original > maximum:
        raise ValueError("compressed payload exceeds decompressed limit")
    if algorithm not in (1, 2):
        raise ValueError("unsupported compressed payload encoding")
    compressed = payload[len(_COMPRESSION_MAGIC) + 5:]
    if algorithm == 1:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
            decoded = stream.read(maximum + 1)
    else:
        try:
            decoded = _zstd_decompressor(maximum).decompress(compressed, max_output_size=maximum + 1)
        except zstandard.ZstdError as exception:
            raise ValueError("invalid Zstandard payload") from exception
    if len(decoded) != original or len(decoded) > maximum:
        raise ValueError("invalid decompressed payload length")
    return decoded


class SpruceError(Exception):
    def __init__(self, status_code: int, status: str, code: str = "", body: str = "", retry_after: float = 0.0) -> None:
        super().__init__(f"Spruce {status_code} {status}: {code or body}")
        self.status_code, self.status, self.code, self.body = status_code, status, code, body
        self.retry_after = retry_after


class HandlerPanicError(RuntimeError):
    pass


class HandlerDrainTimeoutError(TimeoutError):
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
    compression: str = ""


@dataclass(frozen=True)
class PublishResult:
    id: str
    replicated: bool = False

@dataclass(frozen=True)
class BatchEntry:
    payload: bytes
    key: str = ""


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
    cursor: str = ""


@dataclass(frozen=True)
class SubscribeOptions:
    topic: str
    group: str = ""
    since: int = 0
    cursor: str = ""
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
            retry_after = self._parse_retry_after(exc.headers.get("Retry-After", ""))
            error = SpruceError(exc.code, exc.reason, code, raw.strip(), retry_after)
            self._emit(operation, started, status, error)
            raise error from exc
        except BaseException as exc:
            self._emit(operation, started, status, exc)
            raise

    @staticmethod
    def _parse_retry_after(value: str) -> float:
        try: return max(0.0, float(value))
        except (TypeError, ValueError):
            try: return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
            except (TypeError, ValueError, OverflowError): return 0.0

    @staticmethod
    def _retry_delay(backoff: float, maximum: float, retry_after: float = 0.0) -> float:
        return max(retry_after, min(maximum, random.uniform(backoff / 2, backoff * 1.5)))

    @staticmethod
    def _option_headers(options: PublishOptions) -> dict[str, str]:
        values = {"Spruce-Key": options.key, "Content-Type": options.content_type, "Spruce-Producer-ID": options.producer_id, "Spruce-Idempotency-Key": options.idempotency_key, "Spruce-Ack": options.ack, "Spruce-TTL": options.ttl}
        return {key: value for key, value in values.items() if value}

    def publish(self, topic: str, payload: bytes, options: PublishOptions = PublishOptions()) -> PublishResult:
        if not topic: raise ValueError("topic is required")
        if len(payload) > MAX_MESSAGE_BYTES: raise ValueError("payload exceeds 1 MiB")
        path = f"/v1/topics/{urllib.parse.quote(topic, safe='')}/messages"
        encoded = _compress_payload(bytes(payload), options.compression)
        with self._request("POST", path, encoded, self._option_headers(options), operation="publish") as response:
            value = json.load(response)
        return PublishResult(value["id"], bool(value.get("replicated", False)))

    def publish_with_retry(self, topic: str, payload: bytes, options: PublishOptions, retry: RetryOptions = RetryOptions()) -> PublishResult:
        if not options.producer_id or not options.idempotency_key: raise ValueError("retry requires producer ID and idempotency key")
        if retry.max_attempts < 1 or retry.min_backoff < 0 or retry.max_backoff < retry.min_backoff: raise ValueError("invalid retry options")
        delay = retry.min_backoff
        for attempt in range(retry.max_attempts):
            retry_after = 0.0
            try: return self.publish(topic, payload, options)
            except SpruceError as exc:
                if exc.status_code not in (408, 429, 503) or attempt + 1 == retry.max_attempts: raise
                retry_after = exc.retry_after
            except (OSError, urllib.error.URLError):
                if attempt + 1 == retry.max_attempts: raise
            time.sleep(self._retry_delay(delay, retry.max_backoff, retry_after)); delay = min(retry.max_backoff, delay * 2)
        raise RuntimeError("unreachable")

    def publish_batch(self, topic: str, payloads: Sequence[bytes], options: PublishOptions = PublishOptions()) -> list[PublishResult]:
        return self.publish_batch_entries(topic, [BatchEntry(bytes(payload), options.key) for payload in payloads], options)

    def publish_batch_entries(self, topic: str, entries: Sequence[BatchEntry], options: PublishOptions = PublishOptions()) -> list[PublishResult]:
        if not entries or len(entries) > MAX_BATCH_MESSAGES: raise ValueError("batch message count is outside protocol limits")
        if options.idempotency_key or options.ack not in ("", "local"): raise ValueError("options are incompatible with batch publishing")
        if any(len(entry.payload) > MAX_MESSAGE_BYTES for entry in entries): raise ValueError("batch entry exceeds decompressed payload limit")
        encoded = [(entry.key.encode(), _compress_payload(bytes(entry.payload), options.compression)) for entry in entries]
        total = sum(6 + len(key) + len(payload) for key, payload in encoded)
        if total > MAX_BATCH_BYTES or any(len(key) > 8192 or len(payload) > MAX_MESSAGE_BYTES for key, payload in encoded): raise ValueError("batch exceeds protocol limits")
        body = b"".join(struct.pack(">H", len(key)) + key + struct.pack(">I", len(payload)) + payload for key, payload in encoded)
        path = f"/v1/topics/{urllib.parse.quote(topic, safe='')}/batches"
        headers = self._option_headers(replace(options, key="")); headers["Spruce-Batch-Version"] = "2"
        with self._request("POST", path, body, headers, operation="publish_batch") as response:
            ids = json.load(response).get("ids", [])
        if len(ids) != len(entries): raise ValueError("Spruce returned an invalid batch result count")
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
        payload = _decompress_payload(self._read_exact(stream, payload_length), maximum)
        return Delivery(metadata.get("delivery_id", ""), metadata.get("message_id", ""), metadata.get("topic", ""), payload, metadata.get("key", ""), metadata.get("headers", {}), metadata.get("created_at", 0), metadata.get("attempt", 0), metadata.get("cursor", ""))

    def subscribe(self, options: SubscribeOptions, handler: Callable[[Delivery], object], stop: threading.Event | None = None) -> None:
        """Consume until stopped; handlers must be bounded or cooperatively cancellable.

        Python cannot forcibly terminate a running synchronous handler. drain_timeout
        bounds subscribe(), but a non-cooperative handler retains its worker until it
        returns.
        """
        if not options.topic: raise ValueError("topic is required")
        if options.since: raise ValueError("timestamp subscription cursors are no longer supported; use cursor")
        if not 1 <= options.concurrency <= 1024 or not 1 <= options.max_payload_bytes <= 64 << 20: raise ValueError("invalid subscription limits")
        stop = stop or threading.Event(); cursor, backoff = options.cursor, 0.05
        acks, nacks = _AckBatcher(self, "ack"), _AckBatcher(self, "nack")
        workers = ThreadPoolExecutor(max_workers=options.concurrency)
        try:
          while not stop.is_set():
            retry_after = 0.0
            query = {"topic": options.topic};
            if options.group: query["group"] = options.group
            if cursor: query["cursor"] = cursor
            futures: list[tuple[int, object]] = []; sequence, next_progress, completed = 0, 1, {}
            completion_capacity = options.concurrency * 2
            def advance() -> None:
                nonlocal cursor, next_progress
                while next_progress in completed:
                    value = completed.pop(next_progress)
                    if value: cursor = value
                    next_progress += 1
            try:
                connected_at, connected_cursor = time.monotonic(), cursor
                response = self._request("GET", "/v1/subscriptions/stream?" + urllib.parse.urlencode(query), timeout=None, operation="subscribe")
                if not cursor: cursor = response.headers.get("Spruce-Cursor", "")
                self._emit("subscription_connected", connected_at, 200, None)
                connection_done = threading.Event()
                def interrupt() -> None:
                    while not connection_done.wait(.05):
                        if stop.is_set():
                            response.close()
                            return
                threading.Thread(target=interrupt, name="spruce-subscription-interrupt", daemon=True).start()
                accept_completions = threading.Event()
                accept_completions.set()
                try:
                  stream_error = None
                  try:
                    with response:
                      while not stop.is_set():
                          while sequence - next_progress + 1 >= completion_capacity and not stop.is_set():
                              done, _ = wait([future for _, future in futures], timeout=.05, return_when=FIRST_COMPLETED)
                              if not done: continue
                              pending = []
                              for index, future in futures:
                                  if future in done: completed[index] = future.result()
                                  else: pending.append((index, future))
                              futures = pending; advance()
                          delivery = self._read_delivery(response, options.max_payload_bytes)
                          if not delivery.delivery_id: continue
                          sequence += 1
                          def consume(item=delivery):
                              try: handler(item)
                              except Exception:
                                  if accept_completions.is_set(): nacks.submit(item.delivery_id)
                                  return item.cursor
                              except BaseException as exc:
                                  if accept_completions.is_set(): nacks.submit(item.delivery_id)
                                  raise HandlerPanicError(str(exc)) from exc
                              if accept_completions.is_set(): acks.submit(item.delivery_id)
                              return item.cursor
                          futures.append((sequence, workers.submit(consume)))
                          while sequence - next_progress + 1 >= completion_capacity and not stop.is_set():
                              done, _ = wait([future for _, future in futures], timeout=.05, return_when=FIRST_COMPLETED)
                              if not done: continue
                              pending = []
                              for index, future in futures:
                                  if future in done: completed[index] = future.result()
                                  else: pending.append((index, future))
                              futures = pending; advance()
                  except BaseException as exc:
                      stream_error = exc
                  finally:
                      connection_done.set()
                  # Drain before executor shutdown: __exit__ waits without a bound.
                  deadline = time.monotonic() + options.drain_timeout
                  for index, future in futures:
                      try:
                          completed[index] = future.result(timeout=max(0, deadline - time.monotonic()))
                          advance()
                      except TimeoutError as exc:
                          if future.done():
                              raise
                          raise HandlerDrainTimeoutError("Spruce handlers did not stop before drain timeout") from exc
                  if stream_error is not None and not stop.is_set():
                      raise stream_error
                finally:
                    accept_completions.clear()
                if cursor != connected_cursor or time.monotonic() - connected_at >= 5.0: backoff = 0.05
            except SpruceError as exc:
                self._emit("subscription_disconnected", connected_at, exc.status_code, exc)
                if 400 <= exc.status_code < 500 and exc.status_code not in (408, 429): raise
                retry_after = exc.retry_after
            except HandlerPanicError: raise
            except HandlerDrainTimeoutError: raise
            except (EOFError, OSError, urllib.error.URLError) as exc:
                self._emit("subscription_disconnected", connected_at, None, exc)
            self._emit("subscription_reconnecting", time.monotonic(), None, None)
            if stop.wait(self._retry_delay(backoff, 2.0, retry_after)): return
            backoff = min(2.0, backoff * 2)
        finally:
            workers.shutdown(wait=False, cancel_futures=True)
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
        self.client, self.action, self.items, self.closed, self.close_lock = client, action, queue.Queue(1024), threading.Event(), threading.Lock()
        self.thread = threading.Thread(target=self._run, daemon=True); self.thread.start()
    def submit(self, delivery_id: str) -> None:
        result: queue.Queue = queue.Queue(1)
        with self.close_lock:
            if self.closed.is_set(): raise RuntimeError("ACK batcher is closed")
            self.items.put((delivery_id, result))
        error = result.get()
        if error: raise error
    def close(self) -> None:
        with self.close_lock:
            self.closed.set()
            self.items.put(None)
        self.thread.join(10)
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
        self.max_entries, self.ttl, self._seen, self._order, self._lock = max_entries, ttl, {}, deque(), threading.Lock()
    def seen(self, message_id: str) -> bool:
        with self._lock:
            now = time.monotonic()
            if self._seen.get(message_id, 0) > now: return True
            until = now + self.ttl; self._seen[message_id] = until; self._order.append((message_id, until))
            while len(self._order) > self.max_entries:
                old, expiry = self._order.popleft()
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
        key_bytes = len(options.key.encode())
        if not topic or len(payload) > MAX_MESSAGE_BYTES or key_bytes > 8192 or len(payload) + key_bytes + 6 > self.options.max_bytes: raise ValueError("invalid batch publish")
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
        pending, pending_bytes, deadline = [], 0, None
        def flush():
            nonlocal pending, pending_bytes, deadline
            if not pending: return None
            batch, pending, pending_bytes, deadline = pending, [], 0, None
            try:
                values = self.client.publish_batch_entries(batch[0][0], [BatchEntry(item[1], item[2].key) for item in batch], replace(batch[0][2], key=""))
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
            item_bytes = 6 + len(item[2].key.encode()) + len(item[1])
            compatible = not pending or (pending[0][0] == item[0] and replace(pending[0][2], key="") == replace(item[2], key=""))
            if pending and (not compatible or len(pending) >= self.options.max_messages or pending_bytes + item_bytes > self.options.max_bytes): flush()
            pending.append(item)
            pending_bytes += item_bytes
            if len(pending) == 1: deadline = time.monotonic() + self.options.max_delay
            if len(pending) >= self.options.max_messages or pending_bytes >= self.options.max_bytes: flush()


__all__ = ["BatchEntry", "BatcherOptions", "BrokerStatus", "Client", "ClientEvent", "ConsumableDelivery", "Deduper", "Delivery", "HandlerDrainTimeoutError", "HandlerPanicError", "ProducerBatcher", "PublishOptions", "PublishResult", "RetryOptions", "SpruceError", "SubscribeOptions", "__version__"]
