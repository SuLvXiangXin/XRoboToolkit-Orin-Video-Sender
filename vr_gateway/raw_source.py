"""Public latest-frame input contract for non-GStreamer camera adapters."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import threading
import time
from typing import Protocol


@dataclass(frozen=True)
class RawVideoFrame:
    """One RGB frame for an appsrc pipeline."""

    data: bytes
    width: int
    height: int
    captured_monotonic_ns: int


class RawPutResult(str, Enum):
    ENQUEUED = "enqueued"
    OVERWROTE = "overwrote"
    CLOSED = "closed"


class RawFrameSource(Protocol):
    """Lifecycle implemented by an external producer such as a ROS bridge."""

    def start(self) -> None: ...

    def stop(self) -> None: ...


class LatestRawFrameQueue:
    """One-frame handoff. Producers never wait for the encoder consumer."""

    def __init__(self, *, metric_prefix: str = "unitree_raw") -> None:
        self._metric_prefix = metric_prefix
        self._condition = threading.Condition()
        self._frame: RawVideoFrame | None = None
        self._closed = False
        self._overwrites = 0

    def put(self, frame: RawVideoFrame) -> RawPutResult:
        with self._condition:
            if self._closed:
                return RawPutResult.CLOSED
            overwritten = self._frame is not None
            if overwritten:
                self._overwrites += 1
            self._frame = frame
            self._condition.notify()
            return RawPutResult.OVERWROTE if overwritten else RawPutResult.ENQUEUED

    def get(self, timeout_s: float) -> RawVideoFrame | None:
        deadline = time.monotonic() + max(0.0, timeout_s)
        with self._condition:
            while self._frame is None and not self._closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return None
                self._condition.wait(remaining)
            frame, self._frame = self._frame, None
            return frame

    def clear(self) -> None:
        with self._condition:
            self._frame = None

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._frame = None
            self._condition.notify_all()

    def snapshot(self) -> dict[str, int | bool]:
        with self._condition:
            return {
                f"{self._metric_prefix}_frame_pending": self._frame is not None,
                f"{self._metric_prefix}_frames_overwritten": self._overwrites,
            }
