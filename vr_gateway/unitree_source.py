from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable

from .config import SourceConfig
from .metrics import Metrics
from .raw_source import LatestRawFrameQueue, RawPutResult, RawVideoFrame


class UnitreeDependencyError(RuntimeError):
    pass


class UnitreeFrameTimeoutError(TimeoutError):
    pass


class UnitreeTrackEndedError(ConnectionError):
    pass


@dataclass(frozen=True)
class UnitreeApi:
    connection_type: type
    local_sta: Any
    robot_busy_error: type[BaseException]
    aes_required_error: type[BaseException]
    aes_rejected_error: type[BaseException]
    port_error: type[BaseException]
    media_stream_error: type[BaseException]


def load_unitree_api() -> UnitreeApi:
    try:
        from aiortc.mediastreams import MediaStreamError
        from unitree_webrtc_connect import (
            AesKeyRejectedError,
            AesKeyRequiredError,
            LocalSignalingPortError,
            RobotBusyError,
            UnitreeWebRTCConnection,
            WebRTCConnectionMethod,
        )
    except ImportError as exc:
        raise UnitreeDependencyError(
            "unitree-webrtc source requires the optional unitree_webrtc_connect "
            "vendor package and aiortc; install the pinned sibling checkout"
        ) from exc
    return UnitreeApi(
        connection_type=UnitreeWebRTCConnection,
        local_sta=WebRTCConnectionMethod.LocalSTA,
        robot_busy_error=RobotBusyError,
        aes_required_error=AesKeyRequiredError,
        aes_rejected_error=AesKeyRejectedError,
        port_error=LocalSignalingPortError,
        media_stream_error=MediaStreamError,
    )


FrameConverter = Callable[[Any, int, int, int], RawVideoFrame]


class UnitreeWebRTCSource:
    """Own the robot WebRTC peer in one asyncio loop on a dedicated thread."""

    def __init__(
        self,
        config: SourceConfig,
        frames: LatestRawFrameQueue,
        metrics: Metrics,
        *,
        api_loader: Callable[[], UnitreeApi] = load_unitree_api,
        connection_factory: Callable[..., Any] | None = None,
        frame_converter: FrameConverter | None = None,
        width: int,
        height: int,
    ) -> None:
        self.config = config
        self.frames = frames
        self.metrics = metrics
        self.api_loader = api_loader
        self.connection_factory = connection_factory
        self.frame_converter = frame_converter or _convert_video_frame
        self.width = int(width)
        self.height = int(height)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._api: UnitreeApi | None = None
        self._last_frame_s = 0.0

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = None
        self.config.validate()
        _resolve_aes_key(self.config)
        self._api = self.api_loader()
        self._stop.clear()
        self.metrics.set("unitree_source_state", "starting")
        self._thread = threading.Thread(
            target=self._thread_main,
            name="unitree-webrtc-source",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        loop = self._loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(lambda: None)
            except RuntimeError:
                pass
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=self.config.unitree_stop_timeout_s)
            if thread.is_alive():
                self.metrics.inc("unitree_stop_timeouts")
                self.metrics.set("unitree_source_state", "stop_timeout")
                return
        self._thread = None
        self.metrics.set("unitree_source_state", "stopped")

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        except BaseException as exc:  # noqa: BLE001
            self.metrics.inc("unitree_worker_crashes")
            self.metrics.set("unitree_last_error_code", "worker_crash")
            self.metrics.set("unitree_last_error", f"{type(exc).__name__}: {exc}")
            self.metrics.set("unitree_source_state", "failed")

    async def _run(self) -> None:
        self._loop = asyncio.get_running_loop()
        assert self._api is not None
        try:
            while not self._stop.is_set():
                self.metrics.inc("unitree_connect_attempts")
                self.metrics.set("unitree_source_state", "connecting")
                try:
                    await self._run_connection(self._api)
                except BaseException as exc:  # noqa: BLE001
                    if self._stop.is_set():
                        break
                    code, message = _classify_connection_error(exc, self._api)
                    self.metrics.inc(f"unitree_errors_{code}")
                    self.metrics.set("unitree_last_error_code", code)
                    self.metrics.set("unitree_last_error", message)
                if self._stop.is_set():
                    break
                self.metrics.inc("unitree_reconnects")
                self.metrics.set("unitree_source_state", "reconnecting")
                await self._wait_or_stop(self.config.unitree_reconnect_delay_s)
        finally:
            self._loop = None
            if self.metrics.snapshot().get("unitree_source_state") != "stop_timeout":
                self.metrics.set("unitree_source_state", "stopped")

    async def _run_connection(self, api: UnitreeApi) -> None:
        aes_key = _resolve_aes_key(self.config)
        factory = self.connection_factory or api.connection_type
        connection = factory(
            api.local_sta,
            ip=self.config.unitree_ip,
            aes_128_key=aes_key,
            enable_audio=False,
            local_signaling_timeout=self.config.unitree_signaling_timeout_s,
        )
        track_started = asyncio.Event()
        track_ended = asyncio.Event()

        async def on_video_track(track) -> None:
            track_started.set()
            self.metrics.set("unitree_source_state", "streaming")
            try:
                await self._consume_track(track, api.media_stream_error)
            finally:
                track_ended.set()

        try:
            await self._connect_or_stop(connection)
            connection.video.add_track_callback(on_video_track)
            connection.video.switchVideoChannel(True)
            self.metrics.inc("unitree_connections")
            self.metrics.set("unitree_last_error_code", None)
            self.metrics.set("unitree_last_error", None)
            self.metrics.set("unitree_source_state", "connected")
            self._last_frame_s = time.monotonic()

            while not self._stop.is_set():
                if track_ended.is_set():
                    raise UnitreeTrackEndedError("Unitree video track ended")
                now = time.monotonic()
                if now - self._last_frame_s > self.config.unitree_frame_timeout_s:
                    phase = "video track" if not track_started.is_set() else "video frame"
                    raise UnitreeFrameTimeoutError(f"Unitree {phase} timed out")
                await asyncio.sleep(0.05)
        finally:
            try:
                if getattr(connection, "video", None) is not None:
                    connection.video.switchVideoChannel(False)
            except Exception:
                pass
            try:
                await asyncio.wait_for(
                    connection.disconnect(),
                    timeout=min(2.0, self.config.unitree_stop_timeout_s),
                )
            except Exception:
                self.metrics.inc("unitree_disconnect_errors")

    async def _connect_or_stop(self, connection) -> None:
        task = asyncio.create_task(connection.connect())
        deadline = time.monotonic() + self.config.unitree_connect_timeout_s
        try:
            while not task.done():
                if self._stop.is_set():
                    task.cancel()
                    raise asyncio.CancelledError()
                if time.monotonic() >= deadline:
                    task.cancel()
                    raise asyncio.TimeoutError()
                await asyncio.sleep(0.05)
            await task
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _consume_track(
        self,
        track,
        media_stream_error: type[BaseException],
    ) -> None:
        while not self._stop.is_set():
            try:
                frame = await track.recv()
            except media_stream_error:
                return
            captured_ns = time.monotonic_ns()
            try:
                raw = self.frame_converter(frame, self.width, self.height, captured_ns)
            except Exception as exc:
                self.metrics.inc("unitree_frame_conversion_errors")
                self.metrics.set("unitree_last_frame_error", f"{type(exc).__name__}: {exc}")
                continue
            result = self.frames.put(raw)
            if result == RawPutResult.CLOSED:
                return
            self._last_frame_s = time.monotonic()
            self.metrics.inc("unitree_frames_received")
            self.metrics.set("last_unitree_frame_monotonic_ns", captured_ns)
            if result == RawPutResult.OVERWROTE:
                self.metrics.inc("unitree_raw_frame_overwrites")

    async def _wait_or_stop(self, duration_s: float) -> None:
        deadline = time.monotonic() + duration_s
        while not self._stop.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(min(0.1, max(0.0, deadline - time.monotonic())))


def _resolve_aes_key(config: SourceConfig) -> str | None:
    value = ""
    if config.unitree_aes_key_file:
        try:
            value = Path(config.unitree_aes_key_file).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ValueError(
                f"unable to read Unitree AES key file: {config.unitree_aes_key_file}"
            ) from exc
    elif config.unitree_aes_key_env:
        value = os.environ.get(config.unitree_aes_key_env, "").strip()
    if not value:
        return None
    try:
        decoded = bytes.fromhex(value)
    except ValueError as exc:
        raise ValueError("Unitree AES key must contain exactly 32 hexadecimal characters") from exc
    if len(decoded) != 16:
        raise ValueError("Unitree AES key must contain exactly 32 hexadecimal characters")
    return value.lower()


def _convert_video_frame(
    frame,
    width: int,
    height: int,
    captured_monotonic_ns: int,
) -> RawVideoFrame:
    converted = frame
    if int(frame.width) != width or int(frame.height) != height:
        converted = frame.reformat(width=width, height=height, format="rgb24")
    array = converted.to_ndarray(format="rgb24")
    data = array.tobytes(order="C")
    expected = width * height * 3
    if len(data) != expected:
        raise ValueError(f"converted Unitree frame has {len(data)} bytes; expected {expected}")
    return RawVideoFrame(
        data=data,
        width=width,
        height=height,
        captured_monotonic_ns=captured_monotonic_ns,
    )


def _classify_connection_error(exc: BaseException, api: UnitreeApi) -> tuple[str, str]:
    if isinstance(exc, api.robot_busy_error):
        return (
            "robot_busy",
            "Unitree robot is busy with another WebRTC client; close the mobile app or other viewer",
        )
    if isinstance(exc, api.aes_required_error):
        return (
            "aes_key_required",
            "Unitree robot requires an AES-128 key; configure the key environment variable or file",
        )
    if isinstance(exc, api.aes_rejected_error):
        return (
            "aes_key_rejected",
            "Unitree robot rejected the configured AES-128 key; verify it matches this robot",
        )
    if isinstance(exc, api.port_error):
        return (
            "signaling_port_unreachable",
            "Unitree signaling ports 9991 and 8081 are unreachable; verify IP, network, and firewall",
        )
    if isinstance(exc, UnitreeFrameTimeoutError):
        return ("frame_timeout", str(exc))
    if isinstance(exc, UnitreeTrackEndedError):
        return ("track_ended", str(exc))
    if isinstance(exc, asyncio.TimeoutError):
        return ("connect_timeout", "Unitree WebRTC connection timed out")
    return ("connection_error", f"{type(exc).__name__}: {exc}")
