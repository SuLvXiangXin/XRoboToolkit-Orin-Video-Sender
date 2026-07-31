from __future__ import annotations

from dataclasses import dataclass
import socket
import threading
import time
from typing import Callable, Protocol

from .metrics import Metrics
from .protocol import pack_video_access_unit
from .queueing import AccessUnit, AccessUnitQueue


@dataclass(frozen=True)
class Destination:
    host: str
    port: int


class SocketLike(Protocol):
    def setsockopt(self, level: int, option: int, value: int) -> None: ...
    def settimeout(self, value: float) -> None: ...
    def sendall(self, data: bytes) -> None: ...
    def shutdown(self, how: int) -> None: ...
    def close(self) -> None: ...


Connector = Callable[[Destination, float], SocketLike]


class VideoTransport:
    def __init__(
        self,
        queue: AccessUnitQueue,
        *,
        request_recovery: Callable[[], None],
        metrics: Metrics | None = None,
        connector: Connector | None = None,
        reconnect_delay_s: float,
        connect_timeout_s: float,
        send_timeout_s: float,
        send_buffer_bytes: int = 256 * 1024,
    ) -> None:
        self.queue = queue
        self.request_recovery = request_recovery
        self.metrics = metrics or Metrics()
        self.connector = connector or _connect
        self.reconnect_delay_s = float(reconnect_delay_s)
        self.connect_timeout_s = float(connect_timeout_s)
        self.send_timeout_s = float(send_timeout_s)
        self.send_buffer_bytes = int(send_buffer_bytes)
        self._condition = threading.Condition()
        self._target: Destination | None = None
        self._generation = 0
        self._active_socket: SocketLike | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.is_running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="video-network", daemon=True)
        self._thread.start()

    def set_target(self, destination: Destination) -> None:
        if not destination.host or not 1 <= destination.port <= 65535:
            raise ValueError(f"invalid video destination: {destination}")
        with self._condition:
            self._target = destination
            self._generation += 1
            self._interrupt_socket_locked()
            self._condition.notify_all()
        self.queue.require_recovery()
        self.request_recovery()

    def clear_target(self) -> None:
        with self._condition:
            self._target = None
            self._generation += 1
            self._interrupt_socket_locked()
            self._condition.notify_all()
        self.queue.require_recovery()

    def stop(self) -> None:
        self._stop.set()
        with self._condition:
            self._target = None
            self._generation += 1
            self._interrupt_socket_locked()
            self._condition.notify_all()
        self.queue.close()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.send_timeout_s + self.connect_timeout_s + 0.5))
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            target, generation = self._wait_for_target()
            if target is None:
                continue
            sock: SocketLike | None = None
            try:
                self.metrics.set("video_waiting_for_recovery", True)
                first_access_unit = self._wait_for_access_unit(generation)
                if first_access_unit is None:
                    continue
                sock = self.connector(target, self.connect_timeout_s)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.send_buffer_bytes)
                sock.settimeout(self.send_timeout_s)
                with self._condition:
                    if self._stop.is_set() or generation != self._generation:
                        _close_socket(sock)
                        continue
                    self._active_socket = sock
                self.metrics.inc("video_connects")
                self.metrics.set("video_connected", True)
                self.metrics.set("video_waiting_for_recovery", False)
                self._send_access_unit(sock, first_access_unit)
                self._send_loop(sock, generation)
            except (OSError, TimeoutError) as exc:
                self.metrics.inc("video_connect_or_send_errors")
                self.metrics.set("video_last_error", str(exc))
            finally:
                with self._condition:
                    if self._active_socket is sock:
                        self._active_socket = None
                if sock is not None:
                    _close_socket(sock)
                self.metrics.set("video_connected", False)
                self.metrics.set("video_waiting_for_recovery", False)
                self.queue.require_recovery()
                self.request_recovery()
            if not self._stop.is_set() and self._is_generation_current(generation):
                self._stop.wait(self.reconnect_delay_s)

    def _send_loop(self, sock: SocketLike, generation: int) -> None:
        while not self._stop.is_set() and self._is_generation_current(generation):
            access_unit = self.queue.get(timeout_s=0.1)
            if access_unit is None:
                if self.queue.consume_recovery_request():
                    self.request_recovery()
                continue
            self._send_access_unit(sock, access_unit)

    def _wait_for_access_unit(self, generation: int) -> AccessUnit | None:
        while not self._stop.is_set() and self._is_generation_current(generation):
            access_unit = self.queue.get(timeout_s=0.1)
            if access_unit is not None:
                return access_unit
            if self.queue.consume_recovery_request():
                self.request_recovery()
        return None

    def _send_access_unit(self, sock: SocketLike, access_unit: AccessUnit) -> None:
        started = time.monotonic()
        sock.sendall(pack_video_access_unit(access_unit.payload))
        self.metrics.inc("video_access_units_sent")
        self.metrics.inc("video_bytes_sent", len(access_unit.payload))
        self.metrics.set("video_last_send_ms", (time.monotonic() - started) * 1000.0)
        sent_ns = time.monotonic_ns()
        self.metrics.set("last_send_monotonic_ns", sent_ns)
        self.metrics.set(
            "capture_to_send_ms",
            max(0.0, (sent_ns - access_unit.captured_monotonic_ns) * 1e-6),
        )

    def _wait_for_target(self) -> tuple[Destination | None, int]:
        with self._condition:
            while self._target is None and not self._stop.is_set():
                self._condition.wait(0.2)
            return self._target, self._generation

    def _is_generation_current(self, generation: int) -> bool:
        with self._condition:
            return self._target is not None and generation == self._generation

    def _interrupt_socket_locked(self) -> None:
        if self._active_socket is not None:
            _close_socket(self._active_socket)
            self._active_socket = None


def _connect(destination: Destination, timeout_s: float) -> SocketLike:
    return socket.create_connection((destination.host, destination.port), timeout=timeout_s)


def _close_socket(sock: SocketLike) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass
