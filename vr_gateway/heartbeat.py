from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import json
import math
import socket
import threading
import time
from typing import Any

from .config import Endpoint
from .metrics import Metrics


MAX_HEARTBEAT_BYTES = 4096
VALID_DECODER_STATES = frozenset(
    {"starting", "playing", "stalled", "disconnected", "error", "stopped"}
)


class HeartbeatError(ValueError):
    pass


@dataclass(frozen=True)
class ReceiverHeartbeat:
    session_id: str
    sequence: int
    displayed_frames: int
    decoded_frames: int
    unity_update_hz: float
    decoder_state: str
    visual_profile_available: bool


def decode_heartbeat(payload: bytes) -> ReceiverHeartbeat:
    if not payload or len(payload) > MAX_HEARTBEAT_BYTES:
        raise HeartbeatError("heartbeat size is invalid")
    try:
        data = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HeartbeatError("heartbeat is not valid JSON") from exc
    if not isinstance(data, dict):
        raise HeartbeatError("heartbeat must be a JSON object")
    if data.get("version") != 1:
        raise HeartbeatError(f"unsupported heartbeat version: {data.get('version')!r}")
    visual_profile_available = data.get("visual_profile_available", False)
    if not isinstance(visual_profile_available, bool):
        raise HeartbeatError("heartbeat visual profile availability must be boolean")
    try:
        heartbeat = ReceiverHeartbeat(
            session_id=str(data["session_id"]),
            sequence=int(data["sequence"]),
            displayed_frames=int(data["displayed_frames"]),
            decoded_frames=int(data["decoded_frames"]),
            unity_update_hz=float(data["unity_update_hz"]),
            decoder_state=str(data["decoder_state"]),
            visual_profile_available=visual_profile_available,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise HeartbeatError("heartbeat fields are invalid") from exc
    if not heartbeat.session_id or heartbeat.sequence < 1:
        raise HeartbeatError("heartbeat session/sequence is invalid")
    if heartbeat.displayed_frames < 0 or heartbeat.decoded_frames < 0:
        raise HeartbeatError("heartbeat frame counters must be non-negative")
    if not math.isfinite(heartbeat.unity_update_hz) or heartbeat.unity_update_hz < 0.0:
        raise HeartbeatError("heartbeat Unity rate is invalid")
    if heartbeat.decoder_state not in VALID_DECODER_STATES:
        raise HeartbeatError(f"heartbeat decoder state is invalid: {heartbeat.decoder_state!r}")
    return heartbeat


class ReceiverHealth:
    def __init__(self, *, heartbeat_stale_s: float, display_stale_s: float) -> None:
        self.heartbeat_stale_s = float(heartbeat_stale_s)
        self.display_stale_s = float(display_stale_s)
        self._lock = threading.Lock()
        self._heartbeat: ReceiverHeartbeat | None = None
        self._address: tuple[str, int] | None = None
        self._last_heartbeat_s: float | None = None
        self._last_display_progress_s: float | None = None
        self._expected_host: str | None = None

    def reset(self, expected_host: str | None = None) -> None:
        try:
            normalized_host = None if expected_host is None else str(ipaddress.ip_address(expected_host))
        except ValueError:
            normalized_host = None
        with self._lock:
            self._heartbeat = None
            self._address = None
            self._last_heartbeat_s = None
            self._last_display_progress_s = None
            self._expected_host = normalized_host

    def update(self, heartbeat: ReceiverHeartbeat, address: tuple[str, int], *, now: float | None = None) -> bool:
        update_time = time.monotonic() if now is None else float(now)
        with self._lock:
            if self._expected_host is not None and address[0] != self._expected_host:
                return False
            previous = self._heartbeat
            new_session = previous is None or previous.session_id != heartbeat.session_id
            if not new_session and heartbeat.sequence <= previous.sequence:
                return False
            if new_session or heartbeat.displayed_frames > previous.displayed_frames:
                self._last_display_progress_s = update_time
            self._heartbeat = heartbeat
            self._address = address
            self._last_heartbeat_s = update_time
            return True

    def snapshot(self, *, now: float | None = None) -> dict[str, Any]:
        current = time.monotonic() if now is None else float(now)
        with self._lock:
            heartbeat = self._heartbeat
            address = self._address
            heartbeat_time = self._last_heartbeat_s
            progress_time = self._last_display_progress_s
            expected_host = self._expected_host
        heartbeat_age = None if heartbeat_time is None else max(0.0, current - heartbeat_time)
        display_age = None if progress_time is None else max(0.0, current - progress_time)
        healthy = bool(
            heartbeat is not None
            and heartbeat_age is not None
            and heartbeat_age <= self.heartbeat_stale_s
            and display_age is not None
            and display_age <= self.display_stale_s
            and heartbeat.decoder_state == "playing"
        )
        return {
            "receiver_healthy": healthy,
            "receiver_address": None if address is None else address[0],
            "receiver_expected_address": expected_host,
            "receiver_session_id": None if heartbeat is None else heartbeat.session_id,
            "receiver_sequence": None if heartbeat is None else heartbeat.sequence,
            "receiver_decoder_state": None if heartbeat is None else heartbeat.decoder_state,
            "receiver_displayed_frames": None if heartbeat is None else heartbeat.displayed_frames,
            "receiver_decoded_frames": None if heartbeat is None else heartbeat.decoded_frames,
            "receiver_unity_update_hz": None if heartbeat is None else heartbeat.unity_update_hz,
            "receiver_visual_profile_available": bool(
                heartbeat is not None and heartbeat.visual_profile_available
            ),
            "heartbeat_age_s": heartbeat_age,
            "display_progress_age_s": display_age,
        }


class HeartbeatListener:
    def __init__(self, endpoint: Endpoint, health: ReceiverHealth, metrics: Metrics) -> None:
        self.endpoint = endpoint
        self.health = health
        self.metrics = metrics
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._socket: socket.socket | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.endpoint.host, self.endpoint.port))
        sock.settimeout(0.2)
        self._socket = sock
        self._thread = threading.Thread(target=self._run, name="receiver-heartbeat", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        sock, self._socket = self._socket, None
        if sock is not None:
            sock.close()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                assert self._socket is not None
                payload, address = self._socket.recvfrom(MAX_HEARTBEAT_BYTES + 1)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                decoded = decode_heartbeat(payload)
                if self.health.update(decoded, address):
                    self.metrics.inc("receiver_heartbeats")
                else:
                    self.metrics.inc("receiver_heartbeat_replays")
            except HeartbeatError:
                self.metrics.inc("receiver_heartbeat_errors")
