import socket
import unittest

from vr_gateway.config import GatewayConfig, SourceKind, parse_endpoint
from vr_gateway.gateway import VideoGateway


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
    def test_raw_appsrc_kind_creates_queue_and_starts_attached_source(self) -> None:
        gateway = VideoGateway(_raw_config())
        queue = gateway.raw_frame_queue
        source = _DummySource()
        gateway.attach_raw_source(source)
        try:
            gateway.start()
            self.assertEqual(source.started, 1)
            snapshot = gateway.health_snapshot()
            self.assertIn("raw_frame_pending", snapshot)
        finally:
            gateway.stop()
        self.assertEqual(source.stopped, 1)
        self.assertIs(gateway.raw_frame_queue, queue)

    def test_attach_raw_source_rejects_non_raw_kinds(self) -> None:
        gateway = VideoGateway(GatewayConfig())
        with self.assertRaises(RuntimeError):
            gateway.attach_raw_source(_DummySource())
        with self.assertRaises(RuntimeError):
            _ = gateway.raw_frame_queue


if __name__ == "__main__":
    unittest.main()
