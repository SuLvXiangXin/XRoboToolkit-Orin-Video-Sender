from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import signal
import sys
import threading
import time

from .config import EncoderKind, GatewayConfig, SourceKind, parse_endpoint
from .gateway import VideoGateway
from .pipeline import available_gstreamer_features, build_pipeline_description, encoder_candidates, resolve_encoder_factory


def main() -> int:
    config, dry_run, run_seconds = _parse_args()
    config.validate()
    if dry_run:
        features = available_gstreamer_features()
        candidates = encoder_candidates(config.stream.encoder, features)
        output = {
            "config": _jsonable(asdict(config)),
            "available_encoder_features": sorted(features),
            "encoder_candidates": [candidate.value for candidate in candidates],
            "pipelines": [
                build_pipeline_description(
                    config.stream,
                    config.source,
                    candidate,
                    encoder_factory=resolve_encoder_factory(candidate, features),
                )
                for candidate in candidates
            ],
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0

    gateway = VideoGateway(config)
    stop = threading.Event()

    def handle_signal(_signum, _frame) -> None:
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    try:
        gateway.start()
    except Exception as exc:
        print(f"vr-video-gateway: {exc}", file=sys.stderr)
        return 2
    started = time.monotonic()
    try:
        while not stop.wait(0.2):
            if run_seconds is not None and time.monotonic() - started >= run_seconds:
                break
    finally:
        gateway.stop()
    return 0


def _parse_args() -> tuple[GatewayConfig, bool, float | None]:
    parser = argparse.ArgumentParser(description="Independent low-latency PICO video gateway")
    parser.add_argument("--source", choices=[value.value for value in SourceKind], default="synthetic")
    parser.add_argument("--source-path", default="")
    parser.add_argument("--uvc-mode", choices=["raw", "mjpeg"], default="raw")
    parser.add_argument("--unitree-ip", default="", help="Unitree robot IPv4 address")
    parser.add_argument(
        "--unitree-aes-key-env",
        default="UNITREE_AES_128_KEY",
        help="environment variable containing the optional per-device AES key",
    )
    parser.add_argument(
        "--unitree-aes-key-file",
        default="",
        help="path to a file containing the optional per-device AES key; takes precedence over env",
    )
    parser.add_argument("--unitree-connect-timeout-s", type=float, default=20.0)
    parser.add_argument("--unitree-signaling-timeout-s", type=float, default=3.0)
    parser.add_argument("--unitree-frame-timeout-s", type=float, default=3.0)
    parser.add_argument("--unitree-reconnect-delay-s", type=float, default=1.0)
    parser.add_argument("--unitree-stop-timeout-s", type=float, default=8.0)
    parser.add_argument("--encoder", choices=[value.value for value in EncoderKind], default="auto")
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument("--height", type=int, default=720)
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--bitrate", type=int, default=4_000_000, help="H.264 bitrate in bits/s")
    parser.add_argument("--gop-frames", type=int, default=30)
    parser.add_argument("--listen", default="0.0.0.0:13579", help="official Unity OPEN_CAMERA listener")
    parser.add_argument("--target", default="", help="direct PICO destination, normally PICO_IP:12345")
    parser.add_argument("--heartbeat-listen", default="0.0.0.0:13580")
    parser.add_argument("--health-listen", default="127.0.0.1:13581")
    parser.add_argument("--ignore-open-camera-video-config", action="store_true")
    parser.add_argument("--connect-timeout-s", type=float, default=1.0)
    parser.add_argument("--send-timeout-s", type=float, default=0.25)
    parser.add_argument("--reconnect-delay-s", type=float, default=0.25)
    parser.add_argument("--queue-items", type=int, default=4)
    parser.add_argument("--queue-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--queue-max-age-ms", type=int, default=150)
    parser.add_argument("--metrics-interval-s", type=float, default=5.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--run-seconds", type=float)
    args = parser.parse_args()

    config = GatewayConfig()
    config.source.kind = SourceKind(args.source)
    config.source.path = args.source_path
    config.source.uvc_mode = args.uvc_mode
    config.source.unitree_ip = args.unitree_ip
    config.source.unitree_aes_key_env = args.unitree_aes_key_env
    config.source.unitree_aes_key_file = args.unitree_aes_key_file
    config.source.unitree_connect_timeout_s = args.unitree_connect_timeout_s
    config.source.unitree_signaling_timeout_s = args.unitree_signaling_timeout_s
    config.source.unitree_frame_timeout_s = args.unitree_frame_timeout_s
    config.source.unitree_reconnect_delay_s = args.unitree_reconnect_delay_s
    config.source.unitree_stop_timeout_s = args.unitree_stop_timeout_s
    config.stream.encoder = EncoderKind(args.encoder)
    config.stream.width = args.width
    config.stream.height = args.height
    config.stream.fps = args.fps
    config.stream.bitrate = args.bitrate
    config.stream.gop_frames = args.gop_frames
    config.stream.queue_items = args.queue_items
    config.stream.queue_bytes = args.queue_bytes
    config.stream.queue_max_age_ms = args.queue_max_age_ms
    config.control = parse_endpoint(args.listen)
    config.heartbeat = parse_endpoint(args.heartbeat_listen)
    config.health = parse_endpoint(args.health_listen)
    config.direct_target = parse_endpoint(args.target) if args.target else None
    config.honor_open_camera_video_config = not args.ignore_open_camera_video_config
    config.connect_timeout_s = args.connect_timeout_s
    config.send_timeout_s = args.send_timeout_s
    config.reconnect_delay_s = args.reconnect_delay_s
    config.metrics_interval_s = args.metrics_interval_s
    return config, bool(args.dry_run), args.run_seconds


def _jsonable(value):
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if hasattr(value, "value"):
        return value.value
    return value


if __name__ == "__main__":
    raise SystemExit(main())
