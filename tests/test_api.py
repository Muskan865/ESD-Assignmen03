# Run: pytest tests -v
import os
import sys
import tempfile

import grpc
import pytest

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import grpc_server  # noqa: E402
import main  # noqa: E402
import orders_pb2  # noqa: E402
import orders_pb2_grpc  # noqa: E402
import shop  # noqa: E402

shop.init_db()
GOOD = {"product_id": 1, "quantity": 2, "card": "4242424242424242"}


@pytest.fixture
def client():
    main.rate_hits.clear()
    return main.app.test_client()


def test_valid_order(client):
    r = client.post("/orders", json=GOOD)
    assert r.status_code == 201
    assert r.json["status"] == "paid" and r.json["total"] == 50.0


@pytest.mark.parametrize("body", [
    {"product_id": 9, "quantity": 1, "card": "4242424242424242"},
    {"product_id": 1, "quantity": 0, "card": "4242424242424242"},
    {"product_id": 1, "quantity": 1, "card": "abc"},
    {},
])
def test_invalid_input(client, body):
    r = client.post("/orders", json=body)
    assert r.status_code == 400
    assert r.json["error"]["code"] == "invalid_input"


def test_repeated_request_is_not_charged_twice(client):
    headers = {"Idempotency-Key": "test-key-1"}
    first = client.post("/orders", json=GOOD, headers=headers)
    second = client.post("/orders", json=GOOD, headers=headers)
    assert first.status_code == 201 and second.status_code == 200
    assert second.headers["Idempotent-Replayed"] == "true"
    assert first.json["id"] == second.json["id"]


def test_declined_card(client):
    r = client.post("/orders", json={**GOOD, "card": "4000000000000002"})
    assert r.status_code == 402
    assert r.json["error"]["code"] == "payment_declined"


def test_not_found_and_cancel(client):
    assert client.get("/orders/999999").status_code == 404
    order_id = client.post("/orders", json=GOOD).json["id"]
    assert client.post(f"/orders/{order_id}/cancel").json["status"] == "cancelled"
    assert client.post(f"/orders/{order_id}/cancel").status_code == 200  # safe to repeat


def test_rate_limit(client, monkeypatch):
    monkeypatch.setattr(shop, "fake_charge", lambda amount, card: True)
    codes = [client.post("/orders", json=GOOD).status_code for _ in range(main.RATE_LIMIT)]
    assert codes == [201] * main.RATE_LIMIT
    last = client.post("/orders", json=GOOD)
    assert last.status_code == 429 and int(last.headers["Retry-After"]) > 0


def test_grpc_same_rules():
    server = grpc_server.serve(port=50999)
    try:
        stub = orders_pb2_grpc.OrderServiceStub(grpc.insecure_channel("127.0.0.1:50999"))
        order = stub.PlaceOrder(orders_pb2.PlaceOrderRequest(product_id=1, quantity=2, card="4242424242424242"),
                                timeout=2)
        assert order.status == "paid" and order.total == 50.0
        with pytest.raises(grpc.RpcError) as e:
            stub.PlaceOrder(orders_pb2.PlaceOrderRequest(product_id=1, quantity=0, card="4242424242424242"),
                            timeout=2)
        assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    finally:
        server.stop(0)
