# Writes envoy/envoy.json for one load-balancing setup, then: docker compose restart envoy
#   python scripts/envoy_config.py a-only         # A only (Part A/B baseline)
#   python scripts/envoy_config.py round-robin    # A, B, C equal weights (Part C)
#   python scripts/envoy_config.py weighted       # A:B:C = 2:1:1
#   python scripts/envoy_config.py least-request  # A, B, C, least request
import json
import os
import sys

SETUPS = {
    "a-only": ("ROUND_ROBIN", {"A": 1}),
    "round-robin": ("ROUND_ROBIN", {"A": 1, "B": 1, "C": 1}),
    "weighted": ("ROUND_ROBIN", {"A": 2, "B": 1, "C": 1}),
    "least-request": ("LEAST_REQUEST", {"A": 1, "B": 1, "C": 1}),
}
HOST = {"A": "esd-shop-1", "B": "esd-shop-2", "C": "esd-shop-3"}  # Docker Compose replica names

# Active health checks: GET /health on the replica's HTTP port, for both clusters.
HEALTH_CHECK = {
    "interval": "2s", "timeout": "1s",
    "unhealthy_threshold": 2, "healthy_threshold": 2,
    "http_health_check": {"path": "/health", "codec_client_type": "HTTP1"},
}


def access_log(listener):
    """Envoy writes one JSON line per request to stdout; Filebeat ships it like the app logs."""
    return [{
        "name": "envoy.access_loggers.stdout",
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StdoutAccessLog",
            "log_format": {"json_format": {
                "time": "%START_TIME%", "level": "INFO", "service": "envoy", "message": "envoy access",
                "listener": listener,
                "request_id": "%REQ(X-REQUEST-ID)%",       # same ID the replica logs
                "replica": "%RESP(X-REPLICA)%",             # which replica answered
                "upstream_host": "%UPSTREAM_HOST%",
                "method": "%REQ(:METHOD)%", "path": "%REQ(:PATH)%",
                "status": "%RESPONSE_CODE%", "grpc_code": "%GRPC_STATUS(SNAKE_STRING)%",
                "duration_ms": "%DURATION%", "response_flags": "%RESPONSE_FLAGS%",
            }},
        },
    }]


def listener(name, port, cluster, http2):
    hcm = {
        "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
        "stat_prefix": name,
        "codec_type": "HTTP2" if http2 else "AUTO",  # gRPC listener is HTTP/2, so Envoy routes each call
        "access_log": access_log(name),
        "route_config": {"virtual_hosts": [{
            "name": name, "domains": ["*"],
            "routes": [{"match": {"prefix": "/"}, "route": {"cluster": cluster, "timeout": "10s"}}],
        }]},
        "http_filters": [{"name": "envoy.filters.http.router", "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"}}],
    }
    return {
        "name": name,
        "address": {"socket_address": {"address": "0.0.0.0", "port_value": port}},
        "filter_chains": [{"filters": [{"name": "envoy.filters.network.http_connection_manager",
                                        "typed_config": hcm}]}],
    }


def cluster(name, port, policy, weights, http2):
    c = {
        "name": name, "type": "STRICT_DNS", "connect_timeout": "1s", "lb_policy": policy,
        "load_assignment": {"cluster_name": name, "endpoints": [{"lb_endpoints": [{
            "endpoint": {
                "address": {"socket_address": {"address": HOST[r], "port_value": port}},
                "health_check_config": {"port_value": 8000},
            },
            "load_balancing_weight": w,
        } for r, w in weights.items()]}]},
        "health_checks": [HEALTH_CHECK],
        # When a replica is recreated it gets a new IP; drop the old IP at once instead of
        # keeping it while it still passes health checks (it may now belong to another replica).
        "ignore_health_on_host_removal": True,
    }
    if http2:  # talk HTTP/2 to the replicas' gRPC port
        c["typed_extension_protocol_options"] = {"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": {
            "@type": "type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions",
            "explicit_http_config": {"http2_protocol_options": {}}}}
    return c


setup = sys.argv[1] if len(sys.argv) > 1 else "a-only"
policy, weights = SETUPS[setup]
config = {
    "admin": {"address": {"socket_address": {"address": "0.0.0.0", "port_value": 9901}}},
    "static_resources": {
        "listeners": [listener("http", 8000, "shop_http", False), listener("grpc", 50051, "shop_grpc", True)],
        "clusters": [cluster("shop_http", 8000, policy, weights, False),
                     cluster("shop_grpc", 50051, policy, weights, True)],
    },
}
path = os.path.join(os.path.dirname(__file__), "..", "envoy", "envoy.json")
with open(path, "w") as f:
    json.dump(config, f, indent=2)
print(f"wrote envoy/envoy.json: {setup} ({policy}, weights {weights}). Now run: docker compose restart envoy")
