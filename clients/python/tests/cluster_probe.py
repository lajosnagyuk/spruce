import os, threading, time
from spruce import Client, SubscribeOptions

url=os.environ["SPRUCE_URL"]; token=os.environ["SPRUCE_TOKEN"]; topic="python-probe-"+str(time.time_ns())
client=Client(url,token=token,allow_insecure_credentials=url.startswith("http://"))
received=set(); stop=threading.Event()
def consume(delivery):
    received.add(delivery.payload)
    if len(received)==100: stop.set()
thread=threading.Thread(target=lambda: client.subscribe(SubscribeOptions(topic,group="python"),consume,stop),daemon=True); thread.start(); time.sleep(.5)
for i in range(100): client.publish(topic,str(i).encode())
thread.join(30)
if len(received)!=100: raise SystemExit(f"expected 100 unique messages, got {len(received)}")
print("python_cluster_round_trip=passed messages=100")
