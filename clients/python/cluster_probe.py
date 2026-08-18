import os
import ssl
import threading
import time
import uuid

from spruce import Client, SubscribeOptions


url = os.environ["SPRUCE_URL"]
token = os.environ["SPRUCE_TOKEN"]
ca_file = os.environ.get("SPRUCE_CA_FILE")
context = ssl.create_default_context(cafile=ca_file) if ca_file else None
client = Client(url, token=token, ssl_context=context)
topic = "python-probe-" + uuid.uuid4().hex
stop = threading.Event()
received = set()


def consume(delivery):
    received.add(delivery.payload)
    if len(received) == 100:
        stop.set()


subscriber = threading.Thread(
    target=client.subscribe,
    args=(SubscribeOptions(topic, group="python", concurrency=16), consume, stop),
    daemon=True,
)
subscriber.start()
time.sleep(0.5)
for value in range(100):
    client.publish(topic, str(value).encode())
subscriber.join(30)
stop.set()
subscriber.join(2)
if subscriber.is_alive():
    raise RuntimeError("Python subscriber did not stop cooperatively")
if len(received) != 100:
    raise RuntimeError(f"expected 100 unique messages, received {len(received)}")
print("Python live cluster probe passed: 100/100 unique group messages")
