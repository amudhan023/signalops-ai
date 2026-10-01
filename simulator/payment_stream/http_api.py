"""The simulator's HTTP surface: Prometheus scrape target plus fault controls.

  GET  /              health and current state
  GET  /metrics       Prometheus exposition
  GET  /deployments   change history, the "what changed recently?" source
  POST /break         cut the connection pool 50 -> 10 (start the incident)
  POST /heal          restore it (end the incident)
  POST /rate?eps=N    change the target event rate live
"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .config import MAX_EVENTS_PER_SECOND
from .model import POOL_BROKEN, POOL_HEALTHY


def make_server(port, workload, registry):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _send(self, code, body, ctype="application/json"):
            if not isinstance(body, bytes):
                body = (json.dumps(body, indent=2) + "\n").encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlsplit(self.path).path
            if path == "/metrics":
                self._send(200, generate_latest(registry), CONTENT_TYPE_LATEST)
            elif path == "/deployments":
                self._send(200, {"deployments": workload.deployments()})
            elif path == "/":
                self._send(200, workload.snapshot())
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            url = urlsplit(self.path)
            if url.path == "/break":
                workload.set_pool(POOL_BROKEN, "v1.42.0", "maxPoolSize: 50 -> 10")
                self._send(200, workload.snapshot())
            elif url.path == "/heal":
                workload.set_pool(POOL_HEALTHY, "v1.42.1", "revert maxPoolSize: 10 -> 50")
                self._send(200, workload.snapshot())
            elif url.path == "/rate":
                raw = parse_qs(url.query).get("eps", [""])[0]
                if not raw.isdigit() or not 1 <= int(raw) <= MAX_EVENTS_PER_SECOND:
                    self._send(400, {"error": "eps must be an integer in 1..%d"
                                              % MAX_EVENTS_PER_SECOND})
                    return
                workload.set_rate(int(raw))
                self._send(200, workload.snapshot())
            else:
                self._send(404, {"error": "not found"})

        def log_message(self, *_):
            pass   # Prometheus scrapes every 15s; access logs would be noise.

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    return server
