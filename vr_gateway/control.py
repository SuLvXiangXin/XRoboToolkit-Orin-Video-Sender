from __future__ import annotations

import socket
import threading
from dataclasses import replace
from typing import Callable

from .config import Endpoint
from .metrics import Metrics
from .protocol import (
    CameraRequest,
    ControlFrameDecoder,
    ProtocolError,
    decode_control_command,
)


class ControlServer:
    """Single-client official Unity camera-control listener."""

    def __init__(
        self,
        endpoint: Endpoint,
        *,
        on_open: Callable[[CameraRequest], None],
        on_close: Callable[[], None],
        metrics: Metrics,
    ) -> None:
        self.endpoint = endpoint
        self.on_open = on_open
        self.on_close = on_close
        self.metrics = metrics
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._listener: socket.socket | None = None
        self._client: socket.socket | None = None
        self._socket_lock = threading.Lock()

    def start(self) -> None:
        if self._thread is not None:
            return
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.endpoint.host, self.endpoint.port))
        listener.listen(1)
        listener.settimeout(0.2)
        self._listener = listener
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="unity-control", daemon=True)
        self._thread.start()
        self.metrics.set("control_listening", True)

    def stop(self) -> None:
        self._stop.set()
        with self._socket_lock:
            client, self._client = self._client, None
            listener, self._listener = self._listener, None
        _close_socket(client)
        _close_socket(listener)
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
        self.metrics.set("control_listening", False)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                assert self._listener is not None
                client, address = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            client.settimeout(0.2)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            with self._socket_lock:
                if self._stop.is_set():
                    _close_socket(client)
                    break
                self._client = client
            self.metrics.inc("control_connections")
            self.metrics.set("control_client", address[0])
            opened = self._serve_client(client, address[0])
            with self._socket_lock:
                if self._client is client:
                    self._client = None
            _close_socket(client)
            self.metrics.set("control_client", None)
            if opened and not self._stop.is_set():
                self.on_close()

    def _serve_client(self, client: socket.socket, peer_ip: str) -> bool:
        decoder = ControlFrameDecoder()
        opened = False
        while not self._stop.is_set():
            try:
                chunk = client.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            if not chunk:
                break
            try:
                for body in decoder.feed(chunk):
                    command = decode_control_command(body)
                    if command.name == "OPEN_CAMERA":
                        request = CameraRequest.decode(command.data)
                        self.metrics.set("control_payload_video_ip", request.ip)
                        if request.ip != peer_ip:
                            self.metrics.inc("control_payload_ip_mismatches")
                        # The accepted control peer is the authoritative PICO
                        # address. Unity's payload can select the wrong local
                        # interface on multi-homed headsets.
                        request = replace(request, ip=peer_ip)
                        self.on_open(request)
                        opened = True
                        self.metrics.inc("open_camera_commands")
                    elif command.name == "CLOSE_CAMERA":
                        self.on_close()
                        opened = False
                        self.metrics.inc("close_camera_commands")
                    else:
                        self.metrics.inc("unknown_control_commands")
            except (ProtocolError, ValueError) as exc:
                self.metrics.inc("control_protocol_errors")
                self.metrics.set("control_last_error", str(exc))
                break
            except Exception as exc:
                self.metrics.inc("control_handler_errors")
                self.metrics.set("control_last_error", str(exc))
                break
        return opened


def _close_socket(sock: socket.socket | None) -> None:
    if sock is None:
        return
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass
