# Spruce Python client

Dependency-free Python 3.11+ client for Spruce. It supports opaque binary publish,
binary batches, bounded automatic producer batching, idempotent retry, concurrent
streaming consumption, batched ACK/NACK, explicit completion, deduplication,
diagnostics, telemetry, Bearer and Basic authentication.

Subscription handlers are synchronous and must be bounded or cooperatively cancellable.
`drain_timeout` bounds `subscribe()` shutdown, but Python cannot forcibly terminate a
handler that never returns; it retains its worker thread until it cooperates. Use
`deliveries()` for caller-controlled completion, or have long-running handlers observe
an application cancellation event.

```python
from spruce import Client, PublishOptions, SubscribeOptions

client = Client("https://spruce.example.com", token="...")
client.publish("orders", b"opaque bytes", PublishOptions(content_type="application/octet-stream"))
client.subscribe(SubscribeOptions("orders", group="billing"), lambda delivery: handle(delivery.payload))
```

Credentials are rejected over plaintext HTTP unless `allow_insecure_credentials=True`
is explicitly selected for isolated development. Pass an `ssl.SSLContext` through
`ssl_context` to trust a private CA or apply a stricter server-certificate policy. The
client does not provide a first-class client-certificate/mTLS configuration API.
