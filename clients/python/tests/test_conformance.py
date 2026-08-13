import io, json, struct, threading, time, unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from spruce import BatcherOptions, Client, Deduper, ProducerBatcher, PublishOptions, SpruceError

class Handler(BaseHTTPRequestHandler):
    requests = 0
    acks = 0
    def log_message(self, *_): pass
    def do_GET(self):
        if self.path == "/v1/status": self.reply(200, {"messages": 1, "cache_accounted_bytes": 2, "cache_limit_bytes": 3, "peers": 1, "consumers": 0, "pending_deliveries": 0})
        elif self.path.startswith("/v1/subscriptions/stream"):
            metadata=json.dumps({"delivery_id":"delivery","message_id":"message","topic":"stream","created_at":1,"attempt":1}).encode(); payload=b"opaque"
            body=struct.pack(">II",len(metadata),len(payload))+metadata+payload
            self.send_response(200); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
        else: self.send_response(204); self.end_headers()
    def do_POST(self):
        size = int(self.headers.get("Content-Length", 0)); body = self.rfile.read(size); Handler.requests += 1
        if self.path.endswith("/ack") or self.path.endswith("/nack"):
            Handler.acks += len(json.loads(body)["delivery_ids"]); self.send_response(204); self.end_headers()
        elif self.path.endswith("/batches"):
            ids=[]; stream=io.BytesIO(body)
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

if __name__ == "__main__": unittest.main()
