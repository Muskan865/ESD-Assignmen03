# Repeatable traffic for HTTP and gRPC: python scripts/traffic.py --seconds 60
import argparse
import os
import random
import sys
import time

import grpc
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
import orders_pb2  # noqa: E402
import orders_pb2_grpc  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--url", default="http://127.0.0.1:8000")
p.add_argument("--grpc", default="127.0.0.1:50051")
p.add_argument("--seconds", type=int, default=60)
args = p.parse_args()

stub = orders_pb2_grpc.OrderServiceStub(grpc.insecure_channel(args.grpc))
random.seed(42)  # same sequence every run
end = time.time() + args.seconds
sent = 0
while time.time() < end:
    roll = random.random()
    card = "4000000000000002" if roll < 0.1 else "4242424242424242"   # 10% declined
    quantity = 0 if 0.1 <= roll < 0.15 else random.randint(1, 3)        # 5% invalid
    product_id = random.choice([1, 2, 3])

    r = requests.post(f"{args.url}/orders", json={"product_id": product_id, "quantity": quantity, "card": card})
    if r.status_code == 201:
        order_id = r.json()["id"]
        if random.random() < 0.2:
            requests.post(f"{args.url}/orders/{order_id}/cancel")
        requests.get(f"{args.url}/orders/{order_id}")

    try:
        stub.PlaceOrder(orders_pb2.PlaceOrderRequest(product_id=product_id, quantity=quantity, card=card), timeout=2)
    except grpc.RpcError:
        pass  # failures are expected for declined/invalid input
    sent += 1
    time.sleep(0.5)
print(f"sent {sent} HTTP + {sent} gRPC orders")
