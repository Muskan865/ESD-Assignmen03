# Fixed-rate load test through Envoy. Sends requests on a timer WITHOUT waiting for responses.
# No retries, no circuit breaker. Test operations are read-only:
#   HTTP: GET /orders/{id}      gRPC: OrderService.GetOrder (one connection for all calls)
#
#   python scripts/loadtest.py --rate 20 --seconds 60 --label baseline
import argparse
import json
import os
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import grpc
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
import orders_pb2  # noqa: E402
import orders_pb2_grpc  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--http", default="http://127.0.0.1:8000")
p.add_argument("--grpc", default="127.0.0.1:50051")
p.add_argument("--rate", type=float, default=20, help="requests per second, per interface")
p.add_argument("--seconds", type=int, default=60)
p.add_argument("--label", default="run")
args = p.parse_args()

# One order to read during the test (writes go through the same rules on every replica).
order_id = requests.post(f"{args.http}/orders", json={"product_id": 1, "quantity": 1, "card": "4242424242424242"},
                         headers={"Idempotency-Key": "loadtest-order"}, timeout=5).json()["id"]

results = {"http": [], "grpc": []}  # (ok, seconds, replica)
lock = threading.Lock()


def record(interface, ok, seconds, replica):
    with lock:
        results[interface].append((ok, seconds, replica or "?"))


pool = ThreadPoolExecutor(max_workers=200)  # lets HTTP requests overlap


def http_call():
    start = time.time()
    try:
        r = requests.get(f"{args.http}/orders/{order_id}", timeout=5)
        record("http", r.status_code == 200, time.time() - start, r.headers.get("X-Replica"))
    except requests.RequestException:
        record("http", False, time.time() - start, None)


channel = grpc.insecure_channel(args.grpc)
stub = orders_pb2_grpc.OrderServiceStub(channel)


def grpc_call():
    start = time.time()
    future = stub.GetOrder.future(orders_pb2.GetOrderRequest(id=order_id), timeout=5)  # non-blocking

    def done(f):
        try:
            f.result()
            replica = dict(f.initial_metadata()).get("x-replica")
            record("grpc", True, time.time() - start, replica)
        except grpc.RpcError:
            record("grpc", False, time.time() - start, None)
    future.add_done_callback(done)


def run(send):
    """Fire `send` every 1/rate seconds for the duration, never waiting for answers."""
    total = int(args.rate * args.seconds)
    t0 = time.time()
    for i in range(total):
        delay = t0 + i / args.rate - time.time()
        if delay > 0:
            time.sleep(delay)
        send()


started = datetime.now(timezone.utc)
senders = [threading.Thread(target=run, args=(lambda: pool.submit(http_call),)),
           threading.Thread(target=run, args=(grpc_call,))]
for t in senders:
    t.start()
for t in senders:
    t.join()
pool.shutdown(wait=True)
time.sleep(5.5)  # let the last gRPC calls finish (timeout is 5 s)
ended = datetime.now(timezone.utc)


def p95(values):
    values = sorted(values)
    return values[int(0.95 * (len(values) - 1))] if values else 0


summary = {"label": args.label, "start": started.isoformat(), "end": ended.isoformat(),
           "target_rps": args.rate, "seconds": args.seconds}
for interface, rows in results.items():
    ok = sum(1 for r in rows if r[0])
    shares = Counter(r[2] for r in rows if r[0])
    summary[interface] = {
        "sent": len(rows), "success": ok, "failed": len(rows) - ok,
        "actual_rps": round(len(rows) / args.seconds, 1),
        "error_rate_pct": round(100 * (len(rows) - ok) / max(len(rows), 1), 1),
        "p50_ms": round(1000 * sorted(r[1] for r in rows)[len(rows) // 2], 1) if rows else 0,
        "p95_ms": round(1000 * p95([r[1] for r in rows]), 1),
        "shares_pct": {k: round(100 * v / max(ok, 1), 1) for k, v in sorted(shares.items())},
    }
    s = summary[interface]
    print(f"{interface.upper():4} sent={s['sent']} ok={s['success']} failed={s['failed']} "
          f"rate={s['actual_rps']}/s p50={s['p50_ms']}ms p95={s['p95_ms']}ms shares={s['shares_pct']}")

os.makedirs("results", exist_ok=True)
with open("results/runs.jsonl", "a") as f:
    f.write(json.dumps(summary) + "\n")
print(f"saved to results/runs.jsonl ({summary['start']} -> {summary['end']})")
