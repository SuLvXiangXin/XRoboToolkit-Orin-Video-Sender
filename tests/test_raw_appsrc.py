import socket
import unittest
from unittest.mock import patch

from vr_gateway.config import GatewayConfig, SourceKind, parse_endpoint
from vr_gateway.gateway import VideoGateway
from vr_gateway.raw_source import RawVideoFrame
from vr_gateway.transport import Destination


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _raw_config() -> GatewayConfig:
    config = GatewayConfig()
    config.source.kind = SourceKind.RAW_APP_SRC
    config.control = parse_endpoint(f"127.0.0.1:{_free_port()}")
    config.heartbeat = parse_endpoint(f"127.0.0.1:{_free_port()}")
    config.health = parse_endpoint(f"127.0.0.1:{_free_port()}")
    return config


class _DummySource:
    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0

    def start(self) -> None:
        self.started += 1

    def stop(self) -> None:
        self.stopped += 1


class RawAppSrcGatewayTest(unittest.TestCase):
    def test_attached_source_runs_only_while_stream_is_open(self) -> None:
        config = _raw_config()
        gateway = VideoGateway(config)
        queue = gateway.raw_frame_queue
        source = _DummySource()
        gateway.attach_raw_source(source)
        with patch.object(gateway, "_start_pipeline_locked"):
            try:
                gateway.start()
                self.assertEqual(source.started, 0)
                gateway.open_stream(Destination("127.0.0.1", 12345), config.stream)
                self.assertEqual(source.started, 1)
                gateway.open_stream(Destination("127.0.0.1", 12345), config.stream)
                self.assertEqual(source.started, 1)
                queue.put(RawVideoFrame(b"rgb", 1, 1, 1))
                gateway.close_stream()
                self.assertEqual(source.stopped, 1)
                self.assertFalse(queue.snapshot()["raw_frame_pending"])
                gateway.open_stream(Destination("127.0.0.1", 12345), config.stream)
                self.assertEqual(source.started, 2)
                snapshot = gateway.health_snapshot()
                self.assertIn("raw_frame_pending", snapshot)
            finally:
                gateway.stop()
        self.assertEqual(source.stopped, 2)
        self.assertIs(gateway.raw_frame_queue, queue)

    def test_pipeline_start_failure_stops_attached_source(self) -> None:
        config = _raw_config()
        gateway = VideoGateway(config)
        source = _DummySource()
        gateway.attach_raw_source(source)
        with patch.object(gateway, "_start_pipeline_locked", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                gateway.open_stream(Destination("127.0.0.1", 12345), config.stream)
            self.assertEqual(source.started, 1)
            self.assertEqual(source.stopped, 1)

    def test_attach_raw_source_rejects_non_raw_kinds(self) -> None:
        gateway = VideoGateway(GatewayConfig())
        with self.assertRaises(RuntimeError):
            gateway.attach_raw_source(_DummySource())
        with self.assertRaises(RuntimeError):
            _ = gateway.raw_frame_queue


if __name__ == "__main__":
    unittest.main()
