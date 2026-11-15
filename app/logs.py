# One JSON object per line on stdout. Docker stores it, Filebeat ships it.
import json
import logging
import os
import socket
import sys
from datetime import datetime, timezone

SERVICE = os.getenv("SERVICE_NAME", "shop")


def find_replica():
    """Replica name A/B/C: which Compose container name (esd-shop-1/2/3) points at my own IP."""
    try:
        my_ip = socket.gethostbyname(socket.gethostname())
        for number, letter in enumerate("ABC", start=1):
            if socket.gethostbyname(f"esd-shop-{number}") == my_ip:
                return letter
    except OSError:
        pass
    return socket.gethostname()  # not in Docker Compose (e.g. tests)


REPLICA = find_replica()


class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "service": SERVICE,
            "replica": REPLICA,
            "message": record.getMessage(),
        }
        entry.update(getattr(record, "fields", {}))
        return json.dumps(entry)


def get_logger():
    log = logging.getLogger(SERVICE)
    if not log.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        log.addHandler(handler)
        log.setLevel(logging.INFO)
    logging.getLogger("werkzeug").setLevel(logging.ERROR)  # hide plain-text access log
    return log
