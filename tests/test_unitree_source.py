import asyncio
import time
import unittest

from vr_gateway.config import GatewayConfig, SourceConfig, SourceKind
from vr_gateway.gateway import VideoGateway
from vr_gateway.metrics import Metrics
from vr_gateway.protocol import CameraRequest
from vr_gateway.unitree_source import (
    LatestRawFrameQueue,
    RawPutResult,
    RawVideoFrame,
    UnitreeApi,
    UnitreeWebRTCSource,
    _classify_connection_error,
)


class FakeMediaStreamError(Exception):
    pass


class FakeRobotBusyError(Exception):
    pass


class FakeAesRequiredError(Exception):
    pass


class FakeAesRejectedError(Exception):
    pass


class FakePortError(Exception):
    pass


FAKE_API = UnitreeApi(
    connection_type=object,
    local_sta="local-sta",
    robot_busy_error=FakeRobotBusyError,
    aes_required_error=FakeAesRequiredError,
    aes_rejected_error=FakeAesRejectedError,
    port_error=FakePortError,
    media_stream_error=FakeMediaStreamError,
)


class FiniteFakeTrack:
    def __init__(self, frames) -> None:
        self.frames = list(frames)

    async def recv(self):
        if not self.frames:
            raise FakeMediaStreamError()
        return self.frames.pop(0)


class BlockingFakeTrack:
    def __init__(self) -> None:
        self.sent = False

    async def recv(self):
        if not self.sent:
            self.sent = True
            return object()
        await asyncio.sleep(3600)


class FakeVideoChannel:
    def __init__(self) -> None:
        self.callbacks = []
        self.tasks = []

    def add_track_callback(self, callback) -> None:
        self.callbacks.append(callback)

    def switchVideoChannel(self, enabled: bool) -> None:
        if enabled:
            for callback in self.callbacks:
                self.tasks.append(asyncio.create_task(callback(BlockingFakeTrack())))
        else:
            for task in self.tasks:
                task.cancel()


class FakeConnection:
    instances = []

    def __init__(self, method, **kwargs) -> None:
        self.method = method
        self.kwargs = kwargs
        self.video = FakeVideoChannel()
        self.isConnected = False
        type(self).instances.append(self)

    async def connect(self) -> None:
        self.isConnected = True

    async def disconnect(self) -> None:
        self.isConnected = False
        if self.video.tasks:
            await asyncio.gather(*self.video.tasks, return_exceptions=True)


class FlakyFakeConnection(FakeConnection):
    attempts = 0

    async def connect(self) -> None:
        type(self).attempts += 1
        if type(self).attempts == 1:
            raise FakeRobotBusyError()
        await super().connect()


class UnitreeSourceTest(unittest.TestCase):
    def test_latest_queue_overwrites_without_waiting_for_consumer(self) -> None:
        frames = LatestRawFrameQueue()
        first = RawVideoFrame(b"first", 1, 1, 1)
        latest = RawVideoFrame(b"latest", 1, 1, 2)

        self.assertEqual(frames.put(first), RawPutResult.ENQUEUED)
        self.assertEqual(frames.put(latest), RawPutResult.OVERWROTE)

        self.assertEqual(frames.get(timeout_s=0.0), latest)
        self.assertEqual(frames.snapshot()["unitree_raw_frames_overwritten"], 1)

    def test_fake_track_keeps_only_latest_raw_frame(self) -> None:
        frames = LatestRawFrameQueue()
        metrics = Metrics()
        source = UnitreeWebRTCSource(
            _source_config(),
            frames,
            metrics,
            api_loader=lambda: FAKE_API,
            width=1,
            height=1,
            frame_converter=lambda value, width, height, captured: RawVideoFrame(
                bytes([value, value, value]),
                width,
                height,
                captured,
            ),
        )

        asyncio.run(
            source._consume_track(
                FiniteFakeTrack([1, 2, 3]),
                FakeMediaStreamError,
            )
        )

        self.assertEqual(frames.get(timeout_s=0.0).data, b"\x03\x03\x03")
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["unitree_frames_received"], 3)
        self.assertEqual(snapshot["unitree_raw_frame_overwrites"], 2)

    def test_worker_runs_on_separate_thread_with_audio_disabled_and_bounded_stop(self) -> None:
        FakeConnection.instances.clear()
        frames = LatestRawFrameQueue()
        metrics = Metrics()
        config = _source_config()
        config.unitree_stop_timeout_s = 1.0
        source = UnitreeWebRTCSource(
            config,
            frames,
            metrics,
            api_loader=lambda: FAKE_API,
            connection_factory=FakeConnection,
            width=1,
            height=1,
            frame_converter=lambda _value, width, height, captured: RawVideoFrame(
                b"\x00\x00\x00",
                width,
                height,
                captured,
            ),
        )

        source.start()
        self.assertTrue(_wait_for(lambda: metrics.snapshot().get("unitree_frames_received") == 1))
        started = time.monotonic()
        source.stop()

        self.assertLess(time.monotonic() - started, 1.0)
        self.assertFalse(source.is_running)
        self.assertTrue(FakeConnection.instances)
        self.assertFalse(FakeConnection.instances[0].kwargs["enable_audio"])
        self.assertEqual(FakeConnection.instances[0].method, "local-sta")

    def test_typed_connection_errors_are_clear_and_aes_is_redacted(self) -> None:
        cases = (
            (FakeRobotBusyError(), "robot_busy", "another WebRTC client"),
            (FakeAesRequiredError(), "aes_key_required", "requires an AES-128 key"),
            (FakeAesRejectedError("secret-prefix"), "aes_key_rejected", "rejected"),
            (FakePortError(), "signaling_port_unreachable", "9991 and 8081"),
        )
        for error, expected_code, expected_text in cases:
            with self.subTest(expected_code):
                code, message = _classify_connection_error(error, FAKE_API)
                self.assertEqual(code, expected_code)
                self.assertIn(expected_text, message)
                self.assertNotIn("secret-prefix", message)

    def test_worker_reconnects_after_retryable_robot_busy_error(self) -> None:
        FlakyFakeConnection.attempts = 0
        frames = LatestRawFrameQueue()
        metrics = Metrics()
        source = UnitreeWebRTCSource(
            _source_config(),
            frames,
            metrics,
            api_loader=lambda: FAKE_API,
            connection_factory=FlakyFakeConnection,
            width=1,
            height=1,
            frame_converter=lambda _value, width, height, captured: RawVideoFrame(
                b"\x00\x00\x00",
                width,
                height,
                captured,
            ),
        )

        source.start()
        try:
            self.assertTrue(
                _wait_for(lambda: metrics.snapshot().get("unitree_frames_received") == 1)
            )
            snapshot = metrics.snapshot()
            self.assertGreaterEqual(snapshot["unitree_reconnects"], 1)
            self.assertGreaterEqual(snapshot["unitree_errors_robot_busy"], 1)
        finally:
            source.stop()

    def test_gateway_health_requires_fresh_streaming_unitree_frames(self) -> None:
        config = GatewayConfig()
        config.source = _source_config()
        gateway = VideoGateway(config)
        gateway.metrics.set("unitree_source_state", "streaming")
        gateway.metrics.set("last_unitree_frame_monotonic_ns", time.monotonic_ns())

        self.assertTrue(gateway.health_snapshot()["source_healthy"])

        stale_ns = time.monotonic_ns() - int(2.0 * 1e9)
        gateway.metrics.set("last_unitree_frame_monotonic_ns", stale_ns)
        self.assertFalse(gateway.health_snapshot()["source_healthy"])

    def test_control_resolved_destination_keeps_unitree_raw_caps(self) -> None:
        config = GatewayConfig()
        config.source = _source_config()
        config.stream.width = 1280
        config.stream.height = 720
        gateway = VideoGateway(config)
        opened = []
        gateway.open_stream = lambda target, stream: opened.append((target, stream))

        gateway.open_from_unity(
            CameraRequest(
                width=640,
                height=360,
                fps=15,
                bitrate=1_000_000,
                enable_mv_hevc=0,
                render_mode=2,
                port=12345,
                camera="H2",
                ip="192.168.123.20",
            )
        )

        self.assertEqual((opened[0][0].host, opened[0][0].port), ("192.168.123.20", 12345))
        self.assertEqual((opened[0][1].width, opened[0][1].height), (1280, 720))
        self.assertEqual(gateway.metrics.snapshot()["unitree_open_camera_video_config_ignored"], 1)


def _source_config() -> SourceConfig:
    return SourceConfig(
        kind=SourceKind.UNITREE_WEBRTC,
        unitree_ip="192.168.123.161",
        unitree_frame_timeout_s=1.0,
        unitree_reconnect_delay_s=0.01,
    )


def _wait_for(predicate, timeout_s: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


if __name__ == "__main__":
    unittest.main()
