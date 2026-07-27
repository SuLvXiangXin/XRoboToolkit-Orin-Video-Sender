import json
import socket
import struct
import threading
import time
import unittest
from pathlib import Path
from urllib.request import urlopen

from vr_gateway.config import EncoderKind, Endpoint, GatewayConfig
from vr_gateway.gateway import VideoGateway
from vr_gateway.h264 import ParameterSetCache
from vr_gateway.pipeline import available_gstreamer_features
from vr_gateway.protocol import CameraRequest, encode_camera_request, encode_control_command


UNITY_HEARTBEAT_FIXTURE = (
    Path(__file__).parent / "fixtures" / "unity_receiver_heartbeat_v1.json"
)


def _has_x264() -> bool:
    try:
        return "x264enc" in available_gstreamer_features()
    except RuntimeError:
        return False


class GatewayEndToEndTest(unittest.TestCase):
    @unittest.skipUnless(_has_x264(), "x264 GStreamer plugin is unavailable")
    def test_unity_open_camera_streams_recovery_au_and_accepts_receiver_heartbeat(self) -> None:
        video_port = _free_port()
        control_port = _free_port()
        heartbeat_port = _free_port()
        health_port = _free_port()
        received: list[bytes] = []
        receiver_ready = threading.Event()
        receiver_done = threading.Event()

        def receive_one_access_unit() -> None:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", video_port))
            listener.listen(1)
            listener.settimeout(3.0)
            receiver_ready.set()
            try:
                client, _address = listener.accept()
                with client:
                    size = struct.unpack(">I", _recv_exact(client, 4))[0]
                    received.append(_recv_exact(client, size))
            finally:
                listener.close()
                receiver_done.set()

        receiver_thread = threading.Thread(target=receive_one_access_unit)
        receiver_thread.start()
        self.assertTrue(receiver_ready.wait(1.0))

        config = GatewayConfig()
        config.control = Endpoint("127.0.0.1", control_port)
        config.heartbeat = Endpoint("127.0.0.1", heartbeat_port)
        config.health = Endpoint("127.0.0.1", health_port)
        config.stream.encoder = EncoderKind.X264
        config.stream.width = 320
        config.stream.height = 240
        config.stream.fps = 10
        config.stream.bitrate = 500_000
        config.stream.gop_frames = 10
        config.metrics_interval_s = 60.0
        gateway = VideoGateway(config)
        control = None
        try:
            gateway.start()
            control = socket.create_connection(("127.0.0.1", control_port), timeout=1.0)
            request = CameraRequest(
                width=320,
                height=240,
                fps=10,
                bitrate=500_000,
                enable_mv_hevc=0,
                render_mode=2,
                port=video_port,
                camera="ZED",
                # Deliberately wrong: the gateway must use the accepted
                # control peer address for the PICO video callback.
                ip="192.0.2.1",
            )
            frame = encode_control_command("OPEN_CAMERA", encode_camera_request(request))
            control.sendall(frame[:5])
            control.sendall(frame[5:])
            self.assertTrue(receiver_done.wait(5.0))
            self.assertTrue(received)
            self.assertTrue(ParameterSetCache.is_recovery_point(received[0]))

            heartbeat = UNITY_HEARTBEAT_FIXTURE.read_bytes().strip()
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as heartbeat_socket:
                heartbeat_socket.sendto(heartbeat, ("127.0.0.1", heartbeat_port))
            self.assertTrue(_wait_for(lambda: gateway.health_snapshot()["receiver_healthy"]))
            with urlopen(f"http://127.0.0.1:{health_port}/healthz", timeout=1.0) as response:
                health = json.loads(response.read())
            self.assertTrue(health["visual_feedback_healthy"])
            self.assertEqual(health["receiver_session_id"], "session-a")
            self.assertEqual(health["receiver_sequence"], 42)
            self.assertEqual(health["receiver_displayed_frames"], 99)
            self.assertEqual(health["receiver_decoded_frames"], 100)
            self.assertTrue(health["receiver_visual_profile_available"])
            self.assertEqual(health["control_payload_ip_mismatches"], 1)
            self.assertEqual(health["control_payload_video_ip"], "192.0.2.1")

            observation_only = json.loads(heartbeat)
            observation_only["sequence"] = 43
            observation_only["decoded_frames"] = 101
            observation_only["displayed_frames"] = 100
            observation_only["visual_profile_available"] = False
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as heartbeat_socket:
                heartbeat_socket.sendto(
                    json.dumps(observation_only).encode("utf-8"),
                    ("127.0.0.1", heartbeat_port),
                )
            self.assertTrue(
                _wait_for(lambda: gateway.health_snapshot()["receiver_sequence"] == 43)
            )
            observation_health = gateway.health_snapshot()
            self.assertTrue(observation_health["stream_active"])
            self.assertTrue(observation_health["receiver_healthy"])
            self.assertFalse(observation_health["receiver_visual_profile_available"])
            self.assertFalse(observation_health["visual_feedback_healthy"])
        finally:
            if control is not None:
                try:
                    control.sendall(encode_control_command("CLOSE_CAMERA", b""))
                except OSError:
                    pass
                control.close()
            gateway.stop()
            receiver_thread.join(1.0)


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    output = bytearray()
    while len(output) < size:
        chunk = sock.recv(size - len(output))
        if not chunk:
            raise ConnectionError("socket closed before complete video frame")
        output.extend(chunk)
    return bytes(output)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for(predicate, timeout_s: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


if __name__ == "__main__":
    unittest.main()
