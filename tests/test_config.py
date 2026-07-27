import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from vr_gateway.config import EncoderKind, GatewayConfig, SourceKind, parse_endpoint
from vr_gateway.pipeline import build_pipeline_description, encoder_candidates


class ConfigTest(unittest.TestCase):
    def test_default_ports_keep_official_compatibility(self) -> None:
        config = GatewayConfig()
        self.assertEqual(config.control.port, 13579)
        self.assertEqual(config.video_port, 12345)
        self.assertEqual(config.heartbeat.port, 13580)

    def test_parse_endpoint_supports_explicit_host_and_port(self) -> None:
        endpoint = parse_endpoint("127.0.0.1:23456")
        self.assertEqual((endpoint.host, endpoint.port), ("127.0.0.1", 23456))

    def test_auto_encoder_candidates_prefer_hardware_then_x264(self) -> None:
        available = {"nvv4l2h264enc", "nvh264enc", "vah264enc", "x264enc"}
        self.assertEqual(
            encoder_candidates(EncoderKind.AUTO, available),
            [EncoderKind.JETSON, EncoderKind.NVENC, EncoderKind.VAAPI, EncoderKind.X264],
        )

    def test_synthetic_pipeline_has_latest_raw_queue_and_no_network_sink(self) -> None:
        config = GatewayConfig()
        pipeline = build_pipeline_description(config.stream, config.source, EncoderKind.X264)
        self.assertIn("videotestsrc", pipeline)
        self.assertIn("max-size-buffers=1", pipeline)
        self.assertIn("leaky=downstream", pipeline)
        self.assertIn("appsink name=encoded_sink", pipeline)
        self.assertNotIn("tcpclientsink", pipeline)

    def test_file_source_requires_existing_regular_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = GatewayConfig()
            config.source.kind = SourceKind.FILE
            config.source.path = str(Path(tmp) / "missing.mp4")
            with self.assertRaises(ValueError):
                config.validate()

    def test_file_pipeline_requests_any_raw_video_caps(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".mp4") as source_file:
            config = GatewayConfig()
            config.source.kind = SourceKind.FILE
            config.source.path = source_file.name

            pipeline = build_pipeline_description(
                config.stream,
                config.source,
                EncoderKind.X264,
            )

        self.assertIn('caps="video/x-raw(ANY)"', pipeline)
        self.assertIn("expose-all-streams=false", pipeline)

    def test_unitree_source_requires_ipv4_and_builds_nonblocking_appsrc(self) -> None:
        config = GatewayConfig()
        config.source.kind = SourceKind.UNITREE_WEBRTC
        config.source.unitree_ip = "h2.local"
        with self.assertRaisesRegex(ValueError, "IPv4"):
            config.validate()

        config.source.unitree_ip = "192.168.123.161"
        pipeline = build_pipeline_description(
            config.stream,
            config.source,
            EncoderKind.X264,
        )

        self.assertIn("appsrc name=unitree_appsrc", pipeline)
        self.assertIn("block=false", pipeline)
        self.assertIn("max-buffers=1", pipeline)
        self.assertIn("leaky-type=downstream", pipeline)

    def test_unitree_dry_run_never_prints_aes_key_from_environment(self) -> None:
        secret = "0123456789abcdef0123456789abcdef"
        environment = os.environ.copy()
        environment["UNITREE_AES_128_KEY"] = secret
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "vr_gateway",
                "--source",
                "unitree-webrtc",
                "--unitree-ip",
                "192.168.123.161",
                "--encoder",
                "x264",
                "--dry-run",
            ],
            cwd=Path(__file__).parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(secret, result.stdout)
        self.assertNotIn(secret, result.stderr)


if __name__ == "__main__":
    unittest.main()
