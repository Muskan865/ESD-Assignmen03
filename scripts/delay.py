# Add a fixed delay to every request on one replica (HTTP and gRPC), talking to it directly.
#   python scripts/delay.py B 200   # B waits 200 ms per request
#   python scripts/delay.py B 0     # back to normal
import subprocess
import sys

replica, delay_ms = sys.argv[1].upper(), int(sys.argv[2])
container = f"esd-shop-{'ABC'.index(replica) + 1}"
url = f"http://localhost:8000/admin/fault?delay_ms={delay_ms}&every=1"
code = f"import urllib.request as r; print(r.urlopen(r.Request('{url}', method='POST')).read().decode())"
subprocess.run(["docker", "exec", container, "python", "-c", code], check=True)
