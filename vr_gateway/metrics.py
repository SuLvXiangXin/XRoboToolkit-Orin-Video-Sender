from __future__ import annotations

import threading
import time
from typing import Any


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started = time.monotonic()
        self._values: dict[str, int | float | str | bool | None] = {}

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._values[name] = int(self._values.get(name, 0) or 0) + amount

    def set(self, name: str, value: int | float | str | bool | None) -> None:
        with self._lock:
            self._values[name] = value

    def snapshot(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            result = dict(self._values)
        result["uptime_s"] = max(0.0, time.monotonic() - self._started)
        if extra:
            result.update(extra)
        return result
