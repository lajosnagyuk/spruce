# Spruce Python client

Dependency-free Python 3.11+ client for Spruce. It supports opaque binary publish,
binary batches, bounded automatic producer batching, idempotent retry, concurrent
streaming consumption, batched ACK/NACK, explicit completion, deduplication,
diagnostics, telemetry, Bearer and Basic authentication.

```python
from spruce import Client, PublishOptions, SubscribeOptions

client = Client("https://spruce.example.com", token="...")
client.publish("orders", b"opaque bytes", PublishOptions(content_type="application/octet-stream"))
client.subscribe(SubscribeOptions("orders", group="billing"), lambda delivery: handle(delivery.payload))
```

Credentials are rejected over plaintext HTTP unless `allow_insecure_credentials=True`
is explicitly selected for isolated development. Pass an `ssl.SSLContext` through
`ssl_context` for private roots, mTLS, or stricter trust policy.
