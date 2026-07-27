from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading
from typing import Any, Callable

from .config import Endpoint


class HealthServer:
    def __init__(self, endpoint: Endpoint, snapshot: Callable[[], dict[str, Any]]) -> None:
        self.endpoint = endpoint
        self.snapshot = snapshot
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        snapshot = self.snapshot

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - stdlib callback name
                if self.path not in {"/healthz", "/metrics"}:
                    self.send_error(404)
                    return
                payload = json.dumps(snapshot(), sort_keys=True, separators=(",", ":")).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, _format: str, *_args) -> None:
                return None

        server = HTTPServer((self.endpoint.host, self.endpoint.port), Handler)
        server.timeout = 0.2
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, name="gateway-health", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            server.shutdown()
            server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
