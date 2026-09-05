import queue
import threading
import time
import unittest
from spruce import BatcherOptions, ProducerBatcher, PublishResult


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
