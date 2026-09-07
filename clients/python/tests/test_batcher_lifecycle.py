import gc
import queue
import threading
import time
import unittest
import weakref
from spruce import BatcherOptions, ProducerBatcher, PublishResult, SpruceError


class BatcherLifecycle(unittest.TestCase):
    def test_closed_calls_fail_promptly(self):
        class Client:
            def publish_batch_entries(self, topic, entries, options):
                return [PublishResult("id") for _ in entries]
        batcher = ProducerBatcher(Client())
        batcher.close()
        with self.assertRaises(RuntimeError): batcher.flush(timeout=.02)
        with self.assertRaises(RuntimeError): batcher.publish("t", b"x", timeout=.02)

    def test_close_deadline_with_full_queue_and_concurrent_publishers(self):
        entered, release = threading.Event(), threading.Event()
        class Client:
            def publish_batch_entries(self, topic, entries, options):
                entered.set()
                release.wait(2)
                return [PublishResult("id") for _ in entries]
        batcher = ProducerBatcher(Client(), BatcherOptions(max_messages=1, queue_depth=1))
        results = queue.Queue()
        def publish():
            try: results.put(batcher.publish("t", b"x", timeout=1))
            except Exception as exc: results.put(exc)
        threads = [threading.Thread(target=publish) for _ in range(4)]
        try:
            for thread in threads: thread.start()
            self.assertTrue(entered.wait(1))
            start = time.monotonic()
            with self.assertRaises(TimeoutError): batcher.close(timeout=.02)
            self.assertLess(time.monotonic() - start, .3)
        finally:
            release.set()
            batcher.close(timeout=1)
            for thread in threads: thread.join(1)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(results.qsize(), 4)
        self.assertTrue(all(isinstance(results.get(), (PublishResult, RuntimeError)) for _ in threads))

    def test_invalid_delay_rejected_before_worker_start(self):
        for delay in (float("nan"), float("inf"), -1):
            with self.assertRaises(ValueError): ProducerBatcher(None, BatcherOptions(max_delay=delay))

    def test_close_rejects_waiting_admissions_and_drains_accepted(self):
        entered, release = threading.Event(), threading.Event()
        class Client:
            calls = 0
            def publish_batch_entries(self, topic, entries, options):
                self.calls += 1
                if self.calls == 1:
                    entered.set()
                    release.wait(2)
                return [PublishResult("id") for _ in entries]
        batcher = ProducerBatcher(Client(), BatcherOptions(max_messages=1, queue_depth=1))
        values = queue.Queue()
        def publish(timeout=None):
            try: values.put(batcher.publish("t", b"x", timeout=timeout))
            except BaseException as exc: values.put(exc)
        accepted = threading.Thread(target=publish)
        accepted.start()
        self.assertTrue(entered.wait(1))
        queued = threading.Thread(target=publish)
        queued.start()
        deadline = time.monotonic() + 1
        while not batcher._queue.full() and time.monotonic() < deadline:
            time.sleep(.001)
        self.assertTrue(batcher._queue.full())
        blocked = [threading.Thread(target=publish) for _ in range(3)]
        waiting = set()
        all_waiting = threading.Event()
        original_wait = batcher._producers.wait
        def producer_wait(*args, **kwargs):
            waiting.add(threading.get_ident())
            if len(waiting) == len(blocked):
                all_waiting.set()
            return original_wait(*args, **kwargs)
        batcher._producers.wait = producer_wait
        for thread in blocked: thread.start()
        try:
            self.assertTrue(all_waiting.wait(1))
            with self.assertRaises(TimeoutError): batcher.close(timeout=.02)
            for thread in blocked: thread.join(1)
            self.assertTrue(all(not thread.is_alive() for thread in blocked))
            blocked_values = [values.get(timeout=1) for _ in blocked]
            self.assertTrue(all(isinstance(value, RuntimeError) for value in blocked_values))
        finally:
            release.set()
            accepted.join(1); queued.join(1)
            batcher.close(timeout=1)
        self.assertIsInstance(values.get(timeout=1), PublishResult)
        self.assertIsInstance(values.get(timeout=1), PublishResult)

    def test_failed_publish_does_not_retain_caller_frame_after_progress(self):
        class Marker:
            pass
        class Client:
            failed = True
            def publish_batch_entries(self, topic, entries, options):
                if self.failed:
                    self.failed = False
                    raise RuntimeError("temporary network failure")
                return [PublishResult("ok") for _ in entries]

        batcher = ProducerBatcher(Client(), BatcherOptions(max_messages=1))
        def failed_call():
            marker = Marker()
            reference = weakref.ref(marker)
            try:
                batcher.publish("t", b"failed")
            except RuntimeError:
                pass
            return reference
        reference = failed_call()
        self.assertEqual(batcher.publish("t", b"healthy").id, "ok")
        batcher.flush()
        try:
            for _ in range(3):
                gc.collect()
                if reference() is None:
                    break
                time.sleep(.01)
            self.assertIsNone(reference())
        finally:
            with self.assertRaises(RuntimeError) as raised:
                batcher.close()
            self.assertEqual(str(raised.exception), "temporary network failure")

    def test_close_reconstructs_bounded_fresh_failures(self):
        class Client:
            def publish_batch_entries(self, topic, entries, options):
                raise SpruceError(503, "busy", "overloaded", "retry later", 1.25)
        batcher = ProducerBatcher(Client(), BatcherOptions(max_messages=1))
        try:
            batcher.publish("t", b"x")
        except SpruceError as first:
            original = first
            original_depth = sum(1 for _ in _traceback_frames(first))
        else:
            self.fail("failed batch unexpectedly succeeded")
        self.assertGreater(original_depth, 0)
        self.assertLessEqual(original_depth, 8)
        try:
            failures = []
            depths = []
            for _ in range(100):
                try:
                    batcher.close()
                except SpruceError as raised:
                    failures.append(raised)
                    depths.append(sum(1 for _ in _traceback_frames(raised)))
                else:
                    self.fail("close unexpectedly succeeded")
            self.assertEqual(len({id(exc) for exc in failures}), 100)
            self.assertTrue(all(exc.status_code == 503 and exc.status == "busy" and exc.code == "overloaded" and exc.body == "retry later" and exc.retry_after == 1.25 for exc in failures))
            self.assertTrue(all(depth <= 4 for depth in depths))
            self.assertEqual(str(failures[-1]), str(original))
        finally:
            if batcher._thread.is_alive():
                try: batcher.close()
                except SpruceError: pass

    def test_close_preserves_unknown_failure_as_safe_runtime_error(self):
        class CustomFailure(Exception):
            def __init__(self):
                self.marker = object()
                super().__init__("private details")
        class Client:
            def publish_batch_entries(self, topic, entries, options):
                raise CustomFailure()
        batcher = ProducerBatcher(Client(), BatcherOptions(max_messages=1))
        try:
            with self.assertRaises(CustomFailure): batcher.publish("t", b"x")
            with self.assertRaises(RuntimeError) as raised: batcher.close()
            self.assertEqual(str(raised.exception), "batch publish failed: CustomFailure")
            self.assertNotIsInstance(raised.exception, CustomFailure)
        finally:
            if batcher._thread.is_alive():
                try: batcher.close(timeout=1)
                except RuntimeError: pass

    def test_close_reconstructs_ordinary_error_categories(self):
        for expected in (TimeoutError("late"), ValueError("bad result"), RuntimeError("temporary")):
            with self.subTest(error=type(expected).__name__):
                class Client:
                    def publish_batch_entries(self, topic, entries, options):
                        raise expected
                batcher = ProducerBatcher(Client(), BatcherOptions(max_messages=1))
                try:
                    try: batcher.publish("t", b"x")
                    except type(expected): pass
                    with self.assertRaises(type(expected)) as raised: batcher.close()
                    self.assertEqual(str(raised.exception), str(expected))
                finally:
                    if batcher._thread.is_alive():
                        try: batcher.close(timeout=1)
                        except BaseException: pass

    def test_failed_worker_and_timed_out_handoff_release_exception_graphs(self):
        class Marker:
            pass
        class Failure(Exception):
            def __init__(self, marker):
                self.marker = marker
                super().__init__("failure")
        entered, release = threading.Event(), threading.Event()
        class Client:
            first = True
            reference = None
            def publish_batch_entries(self, topic, entries, options):
                if self.first:
                    self.first = False
                    marker = Marker()
                    self.reference = weakref.ref(marker)
                    entered.set()
                    if not release.wait(2):
                        raise RuntimeError("timed handoff did not release")
                    raise Failure(marker)
                return [PublishResult("ok") for _ in entries]

        client = Client()
        batcher = ProducerBatcher(client, BatcherOptions(max_messages=1))
        timed_out = queue.Queue()
        def timed_publish():
            try:
                batcher.publish("t", b"timed", timeout=.02)
            except queue.Empty:
                timed_out.put(True)
        try:
            caller = threading.Thread(target=timed_publish)
            caller.start()
            self.assertTrue(entered.wait(1))
            caller.join(1)
            self.assertFalse(caller.is_alive())
            self.assertTrue(timed_out.get(timeout=1))
            self.assertIsNotNone(client.reference())
            release.set()
            self.assertEqual(batcher.publish("t", b"healthy").id, "ok")
            batcher.flush()
            for _ in range(20):
                gc.collect()
                if client.reference() is None:
                    break
                time.sleep(.01)
            self.assertIsNone(client.reference())
        finally:
            try: batcher.close(timeout=1)
            except BaseException: pass
        gc.collect()
        self.assertIsNone(client.reference())

    def test_many_publishers_progress_and_close_wakes_waiters(self):
        entered, release = threading.Event(), threading.Event()
        class Client:
            calls = 0
            def publish_batch_entries(self, topic, entries, options):
                self.calls += 1
                if self.calls == 1:
                    entered.set()
                    release.wait(2)
                return [PublishResult("id") for _ in entries]
        batcher = ProducerBatcher(Client(), BatcherOptions(max_messages=1, queue_depth=1))
        results = queue.Queue()
        def publish():
            try: results.put(batcher.publish("t", b"x", timeout=1))
            except BaseException as exc: results.put(exc)
        threads = [threading.Thread(target=publish) for _ in range(12)]
        waiting = set()
        all_waiting = threading.Event()
        original_wait = batcher._producers.wait
        def producer_wait(*args, **kwargs):
            waiting.add(threading.get_ident())
            if len(waiting) >= len(threads) - 2:
                all_waiting.set()
            return original_wait(*args, **kwargs)
        batcher._producers.wait = producer_wait
        try:
            for thread in threads: thread.start()
            self.assertTrue(entered.wait(1))
            self.assertTrue(all_waiting.wait(1))
            release.set()
            for thread in threads: thread.join(1)
            self.assertTrue(all(not thread.is_alive() for thread in threads))
            self.assertEqual(results.qsize(), len(threads))
            values = [results.get() for _ in threads]
            self.assertTrue(all(isinstance(value, PublishResult) for value in values))
        finally:
            release.set()
            try: batcher.close(timeout=1)
            except (RuntimeError, TimeoutError): pass
            for thread in threads: thread.join(1)

    def test_timeout_does_not_copy_payload_while_queue_is_full(self):
        entered, release, copied = threading.Event(), threading.Event(), threading.Event()
        class Payload:
            def __len__(self): return 1
            def __bytes__(self):
                copied.set()
                return b"x"
        class Client:
            def publish_batch_entries(self, topic, entries, options):
                entered.set()
                release.wait(2)
                return [PublishResult("id") for _ in entries]
        batcher = ProducerBatcher(Client(), BatcherOptions(max_messages=1, queue_depth=1))
        first = threading.Thread(target=lambda: batcher.publish("t", b"x"))
        first.start()
        self.assertTrue(entered.wait(1))
        second = threading.Thread(target=lambda: batcher.publish("t", b"y", timeout=1))
        second.start()
        deadline = time.monotonic() + 1
        while not batcher._queue.full() and time.monotonic() < deadline:
            time.sleep(.001)
        self.assertTrue(batcher._queue.full())
        try:
            with batcher._queue.mutex:
                before = list(batcher._queue.queue)
            with self.assertRaises(queue.Full):
                batcher.publish("t", Payload(), timeout=.02)
            self.assertFalse(copied.is_set())
            with batcher._queue.mutex:
                after = list(batcher._queue.queue)
            self.assertEqual(after, before)
        finally:
            release.set()
            first.join(1)
            second.join(1)
            batcher.close(timeout=1)

    def test_fifo_flush_barrier_is_preserved(self):
        calls = []
        admitted = threading.Event()
        class Payload:
            def __len__(self): return 1
            def __bytes__(self):
                admitted.set()
                return b"a"
        class Client:
            def publish_batch_entries(self, topic, entries, options):
                calls.append(len(entries))
                return [PublishResult(str(len(calls))) for _ in entries]
        batcher = ProducerBatcher(Client(), BatcherOptions(max_messages=8, max_delay=1, queue_depth=8))
        try:
            result = {}
            first = threading.Thread(target=lambda: result.setdefault("publish", batcher.publish("t", Payload())))
            first.start()
            self.assertTrue(admitted.wait(1))
            second = threading.Thread(target=lambda: (batcher.flush(), result.setdefault("flush", True)))
            second.start(); first.join(1); second.join(1)
            self.assertFalse(first.is_alive() or second.is_alive())
            self.assertEqual(result.get("flush"), True)
            self.assertEqual(calls, [1])
        finally:
            batcher.close(timeout=1)


def _traceback_frames(exc):
    tb = exc.__traceback__
    while tb:
        yield tb
        tb = tb.tb_next
