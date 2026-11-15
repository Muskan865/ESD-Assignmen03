# Business logic shared by the HTTP API (main.py) and the gRPC service (grpc_server.py):
# products, orders (SQLite), validation, idempotency and a fake payment provider.
import os
import random
import sqlite3
import time

from prometheus_client import Counter, Histogram

from logs import get_logger

DB_PATH = os.getenv("DB_PATH", "shop.db")
log = get_logger()

PRODUCTS = {
    1: ("Keyboard", 25.0),
    2: ("Mouse", 10.0),
    3: ("Monitor", 120.0),
}

# ---- Business metrics (recorded here so HTTP and gRPC both count) ----
ORDERS_PLACED = Counter("orders_placed_total", "Orders paid successfully", ["product"])
PAYMENT_FAILURES = Counter("payment_failures_total", "Orders whose payment was declined")
ORDERS_CANCELLED = Counter("orders_cancelled_total", "Orders cancelled by customers")
REVENUE = Counter("revenue_dollars_total", "Money taken from paid orders", ["product"])
PAYMENT_LATENCY = Histogram("payment_duration_seconds", "Time spent in the payment provider",
                            buckets=[0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1])


# Fault injection (experiments): delay every Nth request on this replica, HTTP and gRPC alike.
FAULT = {"delay_ms": 0, "every": 5}
fault_calls = 0


def inject_delay():
    global fault_calls
    fault_calls += 1
    if FAULT["delay_ms"] and fault_calls % FAULT["every"] == 0:
        time.sleep(FAULT["delay_ms"] / 1000)


class ShopError(Exception):
    """A business error. code is one of: invalid_input, not_found, conflict."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def db():
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as c:
        c.execute(
            "CREATE TABLE IF NOT EXISTS orders ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " product_id INTEGER, quantity INTEGER, total REAL,"
            " status TEXT, created_at REAL, idempotency_key TEXT UNIQUE)"
        )


def fake_charge(amount, card):
    """Pretend to call a payment provider. Cards starting with 4000 are declined."""
    with PAYMENT_LATENCY.time():
        time.sleep(random.uniform(0.05, 0.2))
    return not card.startswith("4000")


def validate(product_id, quantity, card):
    if type(product_id) is not int or product_id not in PRODUCTS:
        raise ShopError("invalid_input", f"product_id must be one of {list(PRODUCTS)}")
    if type(quantity) is not int or not 1 <= quantity <= 10:
        raise ShopError("invalid_input", "quantity must be an integer from 1 to 10")
    if not isinstance(card, str) or not card.isdigit() or not 12 <= len(card) <= 19:
        raise ShopError("invalid_input", "card must be 12-19 digits")


def place_order(product_id, quantity, card, idempotency_key=None, request_id=None):
    """Returns (order, replayed). Same idempotency_key -> same order, card charged once."""
    validate(product_id, quantity, card)
    name, price = PRODUCTS[product_id]
    total = price * quantity

    # Claim the key first by inserting a 'pending' order; a repeat hits the UNIQUE constraint.
    try:
        with db() as c:
            cur = c.execute(
                "INSERT INTO orders (product_id, quantity, total, status, created_at, idempotency_key)"
                " VALUES (?, ?, ?, 'pending', ?, ?)",
                (product_id, quantity, total, time.time(), idempotency_key),
            )
            order_id = cur.lastrowid
    except sqlite3.IntegrityError:
        with db() as c:
            row = c.execute("SELECT * FROM orders WHERE idempotency_key = ?", (idempotency_key,)).fetchone()
        log.info("idempotent replay", extra={"fields": {"request_id": request_id, "order_id": row["id"]}})
        return dict(row), True

    status = "paid" if fake_charge(total, card) else "payment_failed"
    with db() as c:
        c.execute("UPDATE orders SET status = ? WHERE id = ?", (status, order_id))

    if status == "paid":
        ORDERS_PLACED.labels(name).inc()
        REVENUE.labels(name).inc(total)
        log.info("order placed", extra={"fields": {"request_id": request_id, "order_id": order_id, "total": total}})
    else:
        PAYMENT_FAILURES.inc()
        log.warning("payment declined", extra={"fields": {"request_id": request_id, "order_id": order_id}})
    return get_order(order_id), False


def get_order(order_id):
    with db() as c:
        row = c.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    if not row:
        raise ShopError("not_found", f"order {order_id} not found")
    return dict(row)


def cancel_order(order_id, request_id=None):
    order = get_order(order_id)
    if order["status"] == "cancelled":
        return order  # cancelling twice is safe: nothing changes
    if order["status"] != "paid":
        raise ShopError("conflict", f"only paid orders can be cancelled (status is {order['status']})")
    with db() as c:
        c.execute("UPDATE orders SET status = 'cancelled' WHERE id = ?", (order_id,))
    ORDERS_CANCELLED.inc()
    log.info("order cancelled", extra={"fields": {"request_id": request_id, "order_id": order_id}})
    return get_order(order_id)


def count_paid_orders():
    with db() as c:
        return c.execute("SELECT COUNT(*) FROM orders WHERE status = 'paid'").fetchone()[0]
