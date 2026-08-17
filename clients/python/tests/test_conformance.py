import io, json, struct, threading, time, unittest, urllib.error, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from spruce import BatcherOptions, Client, Deduper, HandlerDrainTimeoutError, ProducerBatcher, PublishOptions, SpruceError

class Handler(BaseHTTPRequestHandler):
    requests = 0
    acks = 0
    stream_paths = []
    stream_count = 1
    batch_keys = []
    def log_message(self, *_): pass
    def do_GET(self):
        if self.path == "/v1/status": self.reply(200, {"messages": 1, "cache_accounted_bytes": 2, "cache_limit_bytes": 3, "peers": 1, "consumers": 0, "pending_deliveries": 0})
        elif self.path.startswith("/v1/subscriptions/stream"):
            Handler.stream_paths.append(self.path)
            body=b""
            for index in range(Handler.stream_count):
                metadata=json.dumps({"delivery_id":f"delivery-{index}","message_id":f"message-{index}","topic":"stream","created_at":index+1,"attempt":1}).encode(); payload=b"opaque"
                body+=struct.pack(">II",len(metadata),len(payload))+metadata+payload
            self.send_response(200); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
        else: self.send_response(204); self.end_headers()
    def do_POST(self):
        size = int(self.headers.get("Content-Length", 0)); body = self.rfile.read(size); Handler.requests += 1
        if self.path.endswith("/ack") or self.path.endswith("/nack"):
            Handler.acks += len(json.loads(body)["delivery_ids"]); self.send_response(204); self.end_headers()
        elif self.path.endswith("/batches"):
            ids=[]; stream=io.BytesIO(body)
            Handler.batch_keys=[]
            if self.headers.get("Spruce-Batch-Version") == "2":
                while key_chunk := stream.read(2):
                    key=stream.read(struct.unpack(">H",key_chunk)[0]).decode(); size=struct.unpack(">I",stream.read(4))[0]; stream.read(size)
                    Handler.batch_keys.append(key); ids.append(str(len(ids)))
            else:
                while chunk := stream.read(4): ids.append(str(len(ids))); stream.read(struct.unpack(">I", chunk)[0])
            self.reply(202, {"ids": ids})
        else: self.reply(202, {"id": "id", "replicated": False})
    def reply(self, code, value):
        data=json.dumps(value).encode(); self.send_response(code); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

class Conformance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server=ThreadingHTTPServer(("127.0.0.1",0),Handler); threading.Thread(target=cls.server.serve_forever,daemon=True).start()
        cls.client=Client(f"http://127.0.0.1:{cls.server.server_port}")
    @classmethod
    def tearDownClass(cls): cls.server.shutdown()
    def test_publish_batch_status_and_deduper(self):
        self.assertEqual(self.client.publish("t", b"opaque").id,"id")
        self.assertEqual(len(self.client.publish_batch("t",[b"a",b"b"])),2)
        self.assertEqual(self.client.status().messages,1)
        d=Deduper(2,1); self.assertFalse(d.seen("x")); self.assertTrue(d.seen("x"))
    def test_batcher_coalesces_and_copies(self):
        Handler.requests=0
        with ProducerBatcher(self.client, BatcherOptions(max_messages=8,max_delay=.05)) as batcher:
            threads=[threading.Thread(target=lambda: batcher.publish("t",b"x")) for _ in range(8)]
            [t.start() for t in threads]; [t.join() for t in threads]
        self.assertLess(Handler.requests,8)
    def test_batcher_coalesces_distinct_keys(self):
        Handler.requests=0; Handler.batch_keys=[]
        with ProducerBatcher(self.client, BatcherOptions(max_messages=2,max_delay=.2)) as batcher:
            threads=[threading.Thread(target=lambda key=key: batcher.publish("t",b"x",PublishOptions(key=key))) for key in ("a","b")]
            [t.start() for t in threads]; [t.join() for t in threads]
        self.assertEqual(Handler.requests,1); self.assertEqual(Handler.batch_keys,["a","b"])
    def test_batcher_accounts_for_v2_key_at_max_bytes_boundary(self):
        with ProducerBatcher(self.client, BatcherOptions(max_bytes=10)) as batcher:
            with self.assertRaises(ValueError): batcher.publish("t",b"xx",PublishOptions(key="key"))
    def test_safety_validation(self):
        with self.assertRaises(ValueError): Client("http://example",token="secret")
        with self.assertRaises(ValueError): self.client.publish_with_retry("t",b"x",PublishOptions())
        with self.assertRaises(ValueError): self.client.publish_batch("t",[])
    def test_auth_telemetry_and_structured_error(self):
        events=[]
        client=Client(f"http://127.0.0.1:{self.server.server_port}",token="secret",allow_insecure_credentials=True,on_event=lambda event: (events.append(event), (_ for _ in ()).throw(RuntimeError())))
        self.assertEqual(client.publish("t",b"x").id,"id")
        self.assertEqual(len(events),1); self.assertIsNone(events[0].error)
    def test_stream_ack_and_explicit_completion(self):
        Handler.acks=0; stop=threading.Event()
        def consume(delivery): self.assertEqual(delivery.payload,b"opaque"); stop.set()
        self.client.subscribe(__import__('spruce').SubscribeOptions("stream"),consume,stop)
        self.assertEqual(Handler.acks,1)
        stop=threading.Event(); iterator=self.client.deliveries(__import__('spruce').SubscribeOptions("stream"),stop)
        item=next(iterator); self.assertEqual(Handler.acks,1); item.complete();
        for _ in range(100):
            if Handler.acks==2: break
            time.sleep(.01)
        stop.set(); iterator.close(); self.assertEqual(Handler.acks,2)

    def test_post_stream_ack_failure_reconnects_without_advancing_cursor(self):
        stop=threading.Event(); calls=[]; original=self.client._ack; Handler.stream_paths=[]; Handler.stream_count=2
        def flaky(action, ids):
            calls.append((action, tuple(ids)))
            if len(calls) == 1: raise urllib.error.URLError("transient")
            original(action, ids)
            if len(calls) >= 3: stop.set()
        self.client._ack=flaky
        try:
            def consume(delivery):
                if delivery.delivery_id == "delivery-1": time.sleep(.01)
            self.client.subscribe(__import__('spruce').SubscribeOptions("stream", concurrency=2, drain_timeout=.2), consume, stop)
        finally:
            self.client._ack=original; Handler.stream_count=1
        self.assertGreaterEqual(len(calls),3)
        self.assertGreaterEqual(len(Handler.stream_paths),2)
        self.assertNotIn("since=", Handler.stream_paths[1])

    def test_post_stream_ack_socket_timeout_reconnects(self):
        stop=threading.Event(); calls=[]; original=self.client._ack
        def flaky(action, ids):
            calls.append((action, tuple(ids)))
            if len(calls) == 1: raise TimeoutError("socket timeout")
            original(action, ids); stop.set()
        self.client._ack=flaky
        try:
            self.client.subscribe(__import__('spruce').SubscribeOptions("stream", drain_timeout=.2), lambda _: None, stop)
        finally:
            self.client._ack=original
        self.assertGreaterEqual(len(calls),2)

    def test_noncooperative_handler_returns_but_worker_requires_release(self):
        release=threading.Event(); stop=threading.Event()
        def stuck(_): stop.set(); release.wait(5)
        started=time.monotonic()
        try:
            with self.assertRaises(HandlerDrainTimeoutError):
                self.client.subscribe(__import__('spruce').SubscribeOptions("stream", drain_timeout=.05), stuck, stop)
            self.assertLess(time.monotonic()-started,.5)
        finally:
            release.set()

    def test_interrupt_watchers_are_connection_scoped(self):
        stop=threading.Event(); Handler.stream_paths=[]
        def consume(_):
            if len(Handler.stream_paths) >= 8: stop.set()
        self.client.subscribe(__import__('spruce').SubscribeOptions("stream", drain_timeout=.2), consume, stop)
        time.sleep(.1)
        watchers=[thread for thread in threading.enumerate() if thread.name == "spruce-subscription-interrupt"]
        self.assertEqual(watchers, [])

    def test_cursor_advances_only_through_earlier_success(self):
        stop=threading.Event(); calls=[]; original=self.client._ack; Handler.stream_paths=[]; Handler.stream_count=2
        def ordered(action, ids):
            calls.append((action, tuple(ids)))
            if len(calls) == 2: raise urllib.error.URLError("later ACK failed")
            original(action, ids)
            if len(calls) >= 3: stop.set()
        self.client._ack=ordered
        try:
            def consume(delivery):
                if delivery.delivery_id == "delivery-1": time.sleep(.01)
            self.client.subscribe(__import__('spruce').SubscribeOptions("stream", concurrency=2, drain_timeout=.2), consume, stop)
        finally:
            self.client._ack=original; Handler.stream_count=1
        self.assertGreaterEqual(len(Handler.stream_paths),2)
        query=urllib.parse.parse_qs(urllib.parse.urlsplit(Handler.stream_paths[1]).query)
        self.assertEqual(query.get("since"), ["1"])

    def test_deduper_evicts_oldest_entry(self):
        deduper=Deduper(2,60)
        self.assertFalse(deduper.seen("a")); self.assertFalse(deduper.seen("b"))
        self.assertFalse(deduper.seen("c")); self.assertFalse(deduper.seen("a"))

if __name__ == "__main__": unittest.main()
