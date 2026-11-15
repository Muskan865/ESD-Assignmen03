# gRPC version of the order operations. Uses the same shop.py rules and the same SQLite file as the HTTP API.
# Started by main.py inside every replica, so its metrics appear on the replica's /metrics page.
import time
import uuid
from concurrent import futures

import grpc
from prometheus_client import Counter, Gauge, Histogram

import orders_pb2
import orders_pb2_grpc
import shop
from logs import REPLICA, get_logger

log = get_logger()

GRPC_CALLS = Counter("grpc_server_handled_total", "gRPC calls finished", ["method", "code"])
GRPC_LATENCY = Histogram("grpc_server_handling_seconds", "gRPC call duration (server side)", ["method"],
                         buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 2, 5])
GRPC_IN_FLIGHT = Gauge("grpc_server_in_flight", "gRPC calls currently being handled", ["method"])

GRPC_CODE_FOR = {
    "invalid_input": grpc.StatusCode.INVALID_ARGUMENT,
    "not_found": grpc.StatusCode.NOT_FOUND,
    "conflict": grpc.StatusCode.FAILED_PRECONDITION,
}


def to_proto(order, replayed=False):
    return orders_pb2.Order(id=order["id"], product_id=order["product_id"], quantity=order["quantity"],
                            total=order["total"], status=order["status"], replayed=replayed)


def handle(method, ctx, work):
    """Shared wrapper: request ID, metrics, logging and error mapping for every RPC."""
    # Same correlation ID as HTTP: the caller (or Envoy) sends x-request-id metadata.
    request_id = dict(ctx.invocation_metadata()).get("x-request-id") or str(uuid.uuid4())
    ctx.send_initial_metadata((("x-replica", REPLICA),))  # Envoy logs which replica answered
    ctx.set_trailing_metadata((("x-request-id", request_id),))
    start = time.time()
    code, detail, reply = grpc.StatusCode.OK, "", None
    GRPC_IN_FLIGHT.labels(method).inc()
    try:
        shop.inject_delay()
        reply = work(request_id)
    except shop.ShopError as e:
        code, detail = GRPC_CODE_FOR[e.code], e.message
    except Exception:
        log.exception("unhandled error", extra={"fields": {"request_id": request_id}})
        code, detail = grpc.StatusCode.INTERNAL, "something went wrong"
    finally:
        GRPC_IN_FLIGHT.labels(method).dec()

    duration = time.time() - start
    GRPC_CALLS.labels(method, code.name).inc()
    GRPC_LATENCY.labels(method).observe(duration)
    level = log.error if code == grpc.StatusCode.INTERNAL else log.info
    level("rpc completed", extra={"fields": {
        "request_id": request_id, "method": method, "grpc_code": code.name,
        "duration_ms": round(duration * 1000, 1)}})
    if code != grpc.StatusCode.OK:
        ctx.abort(code, detail)
    return reply


class OrderService(orders_pb2_grpc.OrderServiceServicer):
    def PlaceOrder(self, req, ctx):
        def work(request_id):
            order, replayed = shop.place_order(req.product_id, req.quantity, req.card,
                                               req.idempotency_key or None, request_id)
            if order["status"] == "payment_failed":
                raise shop.ShopError("conflict", f"card was declined (order {order['id']})")
            return to_proto(order, replayed)
        return handle("PlaceOrder", ctx, work)

    def GetOrder(self, req, ctx):
        return handle("GetOrder", ctx, lambda request_id: to_proto(shop.get_order(req.id)))


def serve(port=50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    orders_pb2_grpc.add_OrderServiceServicer_to_server(OrderService(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server
