import os
import ssl
import threading
import time
import uuid

from spruce import Client, PublishOptions, SubscribeOptions


url = os.environ["SPRUCE_URL"]
token = os.environ["SPRUCE_TOKEN"]
ca_file = os.environ.get("SPRUCE_CA_FILE")
context = ssl.create_default_context(cafile=ca_file) if ca_file else None
client = Client(url, token=token, ssl_context=context, allow_insecure_credentials=url.startswith("http://"))
topic = "python-probe-" + uuid.uuid4().hex
stop = threading.Event()
received = set()
expected = {f"{value}:".encode() + bytes([97 + value % 26]) * 4096 for value in range(100)}
subscriber_errors = []


def consume(delivery):
    received.add(delivery.payload)
    if len(received) == 100:
        stop.set()


def subscribe():
    try:
        client.subscribe(SubscribeOptions(topic, group="python", concurrency=16), consume, stop)
    except BaseException as exc:
        subscriber_errors.append(exc)


subscriber = threading.Thread(
    target=subscribe,
    daemon=True,
)
subscriber.start()
time.sleep(0.5)
for value, payload in enumerate(expected):
    client.publish(topic, payload, PublishOptions(compression=("", "gzip", "zstd")[value % 3]))
subscriber.join(30)
stop.set()
subscriber.join(2)
if subscriber.is_alive():
    raise RuntimeError("Python subscriber did not stop cooperatively")
if subscriber_errors:
    raise RuntimeError("Python subscriber failed") from subscriber_errors[0]
if received != expected:
    raise RuntimeError(f"expected 100 exact messages, received {len(received)}")
print("Python live cluster probe passed: 100/100 exact raw, gzip, and zstd messages")
