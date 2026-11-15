# Calls the gRPC PlaceOrder directly.
# Run: python clients/grpc_client.py --product-id 1 --quantity 2 --card 4242424242424242
import argparse
import os
import sys
import uuid

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))  # generated stubs live in app/
import orders_pb2  # noqa: E402
import orders_pb2_grpc  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--target", default="127.0.0.1:50051")
p.add_argument("--product-id", type=int, required=True)
p.add_argument("--quantity", type=int, required=True)
p.add_argument("--card", required=True)
p.add_argument("--key", default="", help="idempotency key")
p.add_argument("--deadline", type=float, default=2.0, help="seconds")
args = p.parse_args()

request_id = str(uuid.uuid4())
with grpc.insecure_channel(args.target) as channel:
    stub = orders_pb2_grpc.OrderServiceStub(channel)
    try:
        order = stub.PlaceOrder(
            orders_pb2.PlaceOrderRequest(product_id=args.product_id, quantity=args.quantity,
                                         card=args.card, idempotency_key=args.key),
            timeout=args.deadline, metadata=[("x-request-id", request_id)])
        print(f"OK request_id={request_id}\n{order}")
    except grpc.RpcError as e:
        print(f"ERROR {e.code().name}: {e.details()} (request_id={request_id})")
        sys.exit(1)
