import threading
import time
import unittest

from vr_gateway.config import EncoderKind, SourceConfig, SourceKind, StreamConfig
from vr_gateway.metrics import Metrics
from vr_gateway.pipeline import GstCapturePipeline, available_gstreamer_features
from vr_gateway.queueing import AccessUnitQueue
from vr_gateway.unitree_source import LatestRawFrameQueue, RawVideoFrame


def _has_x264() -> bool:
    try:
        return "x264enc" in available_gstreamer_features()
    except RuntimeError:
        return False


class SlowAccessUnitQueue(AccessUnitQueue):
    def put(self, access_unit):
        time.sleep(0.08)
        return super().put(access_unit)


class PipelineSmokeTest(unittest.TestCase):
    @unittest.skipUnless(_has_x264(), "x264 GStreamer plugin is unavailable")
    def test_synthetic_source_produces_sps_pps_idr_without_camera(self) -> None:
        stream = StreamConfig(
            width=320,
            height=240,
            fps=10,
            bitrate=500_000,
            gop_frames=10,
        )
        queue = AccessUnitQueue(max_items=4, max_bytes=2 * 1024 * 1024, max_age_s=2.0)
        pipeline = GstCapturePipeline(
            stream,
            SourceConfig(),
            EncoderKind.X264,
            queue,
            Metrics(),
        )
        try:
            pipeline.start()
            deadline = time.monotonic() + 5.0
            access_unit = None
            while access_unit is None and time.monotonic() < deadline:
                access_unit = queue.get(timeout_s=0.2)
            self.assertIsNotNone(access_unit)
            self.assertTrue(access_unit.is_recovery_point)
        finally:
            pipeline.stop()
            queue.close()

    @unittest.skipUnless(_has_x264(), "x264 GStreamer plugin is unavailable")
    def test_slow_encoded_callback_drops_at_raw_queue_not_inside_encoded_gop(self) -> None:
        stream = StreamConfig(width=320, height=240, fps=30, bitrate=500_000, gop_frames=10)
        queue = SlowAccessUnitQueue(max_items=100, max_bytes=8 * 1024 * 1024, max_age_s=5.0)
        metrics = Metrics()
        pipeline = GstCapturePipeline(stream, SourceConfig(), EncoderKind.X264, queue, metrics)
        try:
            pipeline.start()
            time.sleep(0.8)
        finally:
            pipeline.stop()
            queue.close()

        snapshot = metrics.snapshot()
        captured = int(snapshot.get("frames_captured", 0))
        entered_encoder = int(snapshot.get("frames_entered_encoder", 0))
        encoded = int(snapshot.get("access_units_encoded", 0))
        self.assertGreaterEqual(captured, 15)
        self.assertGreater(captured, entered_encoder)
        self.assertGreater(captured, encoded)

    @unittest.skipUnless(_has_x264(), "x264 GStreamer plugin is unavailable")
    def test_unitree_appsrc_produces_sps_pps_idr_from_latest_raw_frames(self) -> None:
        width, height = 64, 48
        stream = StreamConfig(
            width=width,
            height=height,
            fps=20,
            bitrate=250_000,
            gop_frames=10,
        )
        source = SourceConfig(
            kind=SourceKind.UNITREE_WEBRTC,
            unitree_ip="127.0.0.1",
        )
        raw_frames = LatestRawFrameQueue()
        encoded = AccessUnitQueue(max_items=4, max_bytes=2 * 1024 * 1024, max_age_s=2.0)
        pipeline = GstCapturePipeline(
            stream,
            source,
            EncoderKind.X264,
            encoded,
            Metrics(),
            raw_frame_source=raw_frames,
        )
        feeder_stop = threading.Event()

        def feed_frames() -> None:
            value = 0
            while not feeder_stop.wait(0.01):
                value = (value + 1) % 255
                raw_frames.put(
                    RawVideoFrame(
                        data=bytes([value]) * (width * height * 3),
                        width=width,
                        height=height,
                        captured_monotonic_ns=time.monotonic_ns(),
                    )
                )

        feeder = threading.Thread(target=feed_frames)
        feeder.start()
        try:
            pipeline.start()
            deadline = time.monotonic() + 5.0
            access_unit = None
            while access_unit is None and time.monotonic() < deadline:
                access_unit = encoded.get(timeout_s=0.2)
            self.assertIsNotNone(access_unit)
            self.assertTrue(access_unit.is_recovery_point)
        finally:
            feeder_stop.set()
            feeder.join(1.0)
            pipeline.stop()
            raw_frames.close()
            encoded.close()


if __name__ == "__main__":
    unittest.main()
