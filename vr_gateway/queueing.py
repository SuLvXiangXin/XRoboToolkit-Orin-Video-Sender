from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
import threading
import time


@dataclass(frozen=True)
class AccessUnit:
    payload: bytes
    is_recovery_point: bool
    pts_ns: int | None = None
    captured_monotonic_ns: int = field(default_factory=time.monotonic_ns)


class PutResult(str, Enum):
    ENQUEUED = "enqueued"
    DROPPED_WAITING_FOR_IDR = "dropped_waiting_for_idr"
    RECOVERY_REQUIRED = "recovery_required"
    CLOSED = "closed"


class AccessUnitQueue:
    """Bounded encoded queue that never resumes in the middle of a GOP."""

    def __init__(self, *, max_items: int, max_bytes: int, max_age_s: float) -> None:
        if max_items <= 0 or max_bytes <= 0 or max_age_s <= 0.0:
            raise ValueError("queue limits must be positive")
        self.max_items = int(max_items)
        self.max_bytes = int(max_bytes)
        self.max_age_ns = int(max_age_s * 1e9)
        self._items: deque[AccessUnit] = deque()
        self._bytes = 0
        self._waiting_for_recovery = True
        self._recovery_requested = True
        self._closed = False
        self._condition = threading.Condition()

    @property
    def waiting_for_recovery(self) -> bool:
        with self._condition:
            return self._waiting_for_recovery

    def put(self, access_unit: AccessUnit) -> PutResult:
        size = len(access_unit.payload)
        with self._condition:
            if self._closed:
                return PutResult.CLOSED
            if size <= 0 or size > self.max_bytes:
                self._enter_recovery_locked()
                return PutResult.RECOVERY_REQUIRED
            if self._waiting_for_recovery:
                if not access_unit.is_recovery_point:
                    return PutResult.DROPPED_WAITING_FOR_IDR
                self._waiting_for_recovery = False
                self._recovery_requested = False
            if len(self._items) >= self.max_items or self._bytes + size > self.max_bytes:
                self._enter_recovery_locked()
                if not access_unit.is_recovery_point:
                    return PutResult.RECOVERY_REQUIRED
                self._waiting_for_recovery = False
                self._recovery_requested = False
            self._items.append(access_unit)
            self._bytes += size
            self._condition.notify()
            return PutResult.ENQUEUED

    def get(self, timeout_s: float) -> AccessUnit | None:
        deadline = time.monotonic() + max(0.0, timeout_s)
        with self._condition:
            while not self._items and not self._closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return None
                self._condition.wait(remaining)
            if not self._items:
                return None
            oldest = self._items[0]
            if time.monotonic_ns() - oldest.captured_monotonic_ns > self.max_age_ns:
                self._enter_recovery_locked()
                return None
            result = self._items.popleft()
            self._bytes -= len(result.payload)
            return result

    def require_recovery(self) -> None:
        with self._condition:
            self._enter_recovery_locked()

    def consume_recovery_request(self) -> bool:
        with self._condition:
            value = self._recovery_requested
            self._recovery_requested = False
            return value

    def snapshot(self) -> dict[str, int | bool]:
        with self._condition:
            return {
                "encoded_queue_items": len(self._items),
                "encoded_queue_bytes": self._bytes,
                "waiting_for_recovery": self._waiting_for_recovery,
            }

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._items.clear()
            self._bytes = 0
            self._condition.notify_all()

    def _enter_recovery_locked(self) -> None:
        self._items.clear()
        self._bytes = 0
        self._waiting_for_recovery = True
        self._recovery_requested = True
        self._condition.notify_all()
