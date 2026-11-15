# Cardinality experiment. Exposes demo_requests_total on :8001 (Prometheus job "demo").
#   python scripts/cardinality_demo.py --with-id   -> one series per request ID (bad)
#   python scripts/cardinality_demo.py             -> one series only (good)
import argparse
import time
import uuid

from prometheus_client import Counter, start_http_server

p = argparse.ArgumentParser()
p.add_argument("--with-id", action="store_true")
args = p.parse_args()

labels = ["endpoint", "request_id"] if args.with_id else ["endpoint"]
DEMO = Counter("demo_requests_total", "Demo requests", labels)

start_http_server(8001)
for i in range(100):  # stop at 100 unique IDs
    if args.with_id:
        DEMO.labels("/createOrder", str(uuid.uuid4())).inc()
    else:
        DEMO.labels("/createOrder").inc()
    time.sleep(0.05)
print("100 requests recorded; keeping /metrics up for scraping. Ctrl+C to stop.")
while True:
    time.sleep(1)
