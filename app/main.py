# HTTP API for the shop (Flask).
import math
import os
import threading
import time
import uuid

from flask import Flask, Response, g, jsonify, request, send_file
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, Summary, generate_latest
from werkzeug.exceptions import HTTPException

import grpc_server
import shop
from logs import REPLICA, get_logger

app = Flask(__name__)
log = get_logger()

# ---- Application metrics ----
HTTP_REQUESTS = Counter("http_requests_total", "HTTP requests", ["method", "endpoint", "status"])
HTTP_LATENCY = Histogram("http_request_duration_seconds", "HTTP request duration", ["method", "endpoint"],
                         buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 2, 5])
HTTP_IN_FLIGHT = Gauge("http_requests_in_flight", "HTTP requests currently being handled")
ORDER_LATENCY = Summary("order_request_seconds", "Time to handle a create-order request")

# ---- Business metric read straight from the database at scrape time ----
ORDERS_PENDING = Gauge("orders_awaiting_shipment", "Paid orders not yet shipped or cancelled")
ORDERS_PENDING.set_function(shop.count_paid_orders)

# Fault injection: fail the next N POST /orders with 503 (delays live in shop.FAULT).
fail_next = 0

# /metrics and /health (Prometheus scrapes, Envoy health checks) are not counted as traffic.
NOT_TRAFFIC = ("/metrics", "/health")

# Rate limit for POST /orders: RATE_LIMIT requests per RATE_WINDOW seconds per client IP.
RATE_LIMIT, RATE_WINDOW = 30, 10
rate_hits = {}  # ip -> (window_start, count)
rate_lock = threading.Lock()

STATUS_FOR = {"invalid_input": 400, "not_found": 404, "conflict": 409}


def error(status, code, message, headers=None):
    """Every error uses the same body: {"error": {"code": ..., "message": ...}}"""
    return jsonify({"error": {"code": code, "message": message}}), status, headers or {}


@app.errorhandler(shop.ShopError)
def shop_error(e):
    return error(STATUS_FOR[e.code], e.code, e.message)


@app.errorhandler(HTTPException)
def http_error(e):
    return error(e.code, e.name.lower().replace(" ", "_"), e.description)


@app.errorhandler(Exception)
def unexpected_error(e):
    log.exception("unhandled error", extra={"fields": {"request_id": g.get("request_id")}})
    return error(500, "internal_error", "something went wrong")


@app.before_request
def start():
    g.start = time.time()
    g.request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    if request.path not in NOT_TRAFFIC:
        HTTP_IN_FLIGHT.inc()


@app.after_request
def finish(response):
    response.headers["X-Replica"] = REPLICA  # Envoy logs which replica answered
    if request.path in NOT_TRAFFIC:
        return response
    HTTP_IN_FLIGHT.dec()
    duration = time.time() - g.start
    endpoint = request.url_rule.rule if request.url_rule else "unknown"
    HTTP_REQUESTS.labels(request.method, endpoint, str(response.status_code)).inc()
    HTTP_LATENCY.labels(request.method, endpoint).observe(duration)
    level = log.error if response.status_code >= 500 else log.info
    level("request completed", extra={"fields": {
        "request_id": g.request_id, "method": request.method, "path": endpoint,
        "status": response.status_code, "duration_ms": round(duration * 1000, 1)}})
    response.headers["X-Request-ID"] = g.request_id
    return response


def rate_limit_wait():
    """Returns 0 if the call is allowed, otherwise seconds until the window resets."""
    now = time.time()
    # Behind Envoy every request comes from Envoy's IP, so use the original client from X-Forwarded-For.
    ip = request.headers.get("X-Forwarded-For", request.remote_addr).split(",")[0].strip()
    with rate_lock:
        start, count = rate_hits.get(ip, (now, 0))
        if now - start >= RATE_WINDOW:
            start, count = now, 0
        if count >= RATE_LIMIT:
            return math.ceil(start + RATE_WINDOW - now)
        rate_hits[ip] = (start, count + 1)
    return 0


@app.get("/")
def storefront():
    return app.send_static_file("index.html")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.get("/openapi.yaml")
def openapi():
    return send_file(os.path.join(os.path.dirname(__file__), "openapi.yaml"), mimetype="text/yaml")


@app.get("/docs")
def docs():
    return """<!doctype html><html><head><title>Shop API</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css"></head>
<body><div id="ui"></div><script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#ui"});</script></body></html>"""


@app.get("/products")
def list_products():
    return jsonify([{"id": i, "name": n, "price": p} for i, (n, p) in shop.PRODUCTS.items()])


@app.post("/orders")
@ORDER_LATENCY.time()
def create_order():
    wait = rate_limit_wait()
    if wait:
        return error(429, "rate_limited", f"max {RATE_LIMIT} orders per {RATE_WINDOW}s", {"Retry-After": str(wait)})

    global fail_next
    if fail_next > 0:
        fail_next -= 1
        return error(503, "unavailable", "injected failure, try again")
    shop.inject_delay()

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return error(400, "invalid_input", "body must be a JSON object")
    order, replayed = shop.place_order(data.get("product_id"), data.get("quantity"), data.get("card"),
                                       request.headers.get("Idempotency-Key"), g.request_id)
    if order["status"] == "payment_failed":
        return error(402, "payment_declined", f"card was declined (order {order['id']})")
    return jsonify(order), 200 if replayed else 201, {"Idempotent-Replayed": str(replayed).lower()}


@app.get("/orders/<int:order_id>")
def get_order(order_id):
    shop.inject_delay()
    return jsonify(shop.get_order(order_id))


@app.post("/orders/<int:order_id>/cancel")
def cancel_order(order_id):
    return jsonify(shop.cancel_order(order_id, g.request_id))


@app.post("/admin/fault")
def set_fault():
    global fail_next
    shop.FAULT["delay_ms"] = int(request.args.get("delay_ms", 0))
    shop.FAULT["every"] = int(request.args.get("every", 5))
    shop.fault_calls = 0
    fail_next = int(request.args.get("fail_next", 0))
    settings = {**shop.FAULT, "fail_next": fail_next}
    log.warning("fault settings changed", extra={"fields": settings})
    return settings


if __name__ == "__main__":
    shop.init_db()
    grpc = grpc_server.serve(50051)  # gRPC runs in the same process (background threads), same /metrics
    log.info("replica started: HTTP :8000, gRPC :50051")
    app.run(host="0.0.0.0", port=8000, threaded=True)
