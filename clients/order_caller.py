# Acts like another service calling POST /orders, with timeout, retry+backoff and a circuit breaker.
# Run: python clients/order_caller.py   (the demo switches the server's fault injection on and off)
import time
import uuid

import requests

URL = "http://127.0.0.1:8000"
TIMEOUT = 2.0          # seconds per HTTP attempt
MAX_RETRIES = 2        # so at most 3 attempts per call
BACKOFF = 0.5          # 0.5 s, then 1 s
BREAKER_FAILURES = 3   # open after 3 failed calls in a row
BREAKER_RESET = 5.0    # stay open 5 s, then allow one trial call


class CircuitBreaker:
    def __init__(self):
        self.failures = 0
        self.opened_at = None

    def state(self):
        if self.opened_at is None:
            return "CLOSED"
        return "HALF-OPEN" if time.time() - self.opened_at >= BREAKER_RESET else "OPEN"

    def success(self):
        self.failures, self.opened_at = 0, None

    def failure(self):
        self.failures += 1
        if self.failures >= BREAKER_FAILURES or self.opened_at is not None:
            self.opened_at = time.time()


breaker = CircuitBreaker()


def place_order(product_id, quantity, card):
    if breaker.state() == "OPEN":
        print("  breaker OPEN -> call blocked, server not contacted")
        return None
    key = str(uuid.uuid4())  # same key on every retry, so a retry can never charge twice
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            r = requests.post(f"{URL}/orders", json={"product_id": product_id, "quantity": quantity, "card": card},
                              headers={"Idempotency-Key": key, "X-Request-ID": key}, timeout=TIMEOUT)
            if r.status_code < 500:
                breaker.success()
                print(f"  attempt {attempt}: HTTP {r.status_code} {r.json()}")
                return r
            reason = f"HTTP {r.status_code}"
        except requests.Timeout:
            reason = "timeout"
        except requests.ConnectionError:
            reason = "connection error"
        print(f"  attempt {attempt}: failed ({reason})")
        if attempt <= MAX_RETRIES:
            time.sleep(BACKOFF * 2 ** (attempt - 1))
    breaker.failure()
    print(f"  call failed after retries, breaker is {breaker.state()}")
    return None


def fault(**params):
    requests.post(f"{URL}/admin/fault", params=params, timeout=TIMEOUT)


if __name__ == "__main__":
    fault()
    print("1) normal call")
    place_order(1, 1, "4242424242424242")

    print("2) transient failure: next request fails once, retry succeeds")
    fault(fail_next=1)
    place_order(2, 1, "4242424242424242")

    print("3) outage: every request fails -> breaker opens and blocks calls")
    fault(fail_next=1000)
    for _ in range(5):
        place_order(3, 1, "4242424242424242")

    print(f"4) outage fixed, waiting {BREAKER_RESET}s for breaker recovery period")
    fault()
    time.sleep(BREAKER_RESET)
    print(f"  breaker is {breaker.state()}")
    place_order(1, 1, "4242424242424242")
    print(f"  breaker is {breaker.state()}")
    place_order(2, 2, "4242424242424242")
