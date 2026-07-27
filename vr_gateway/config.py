from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import ipaddress
from pathlib import Path

from .protocol import CONTROL_PORT, HEARTBEAT_PORT, HEALTH_PORT, VIDEO_PORT


class SourceKind(str, Enum):
    SYNTHETIC = "synthetic"
    FILE = "file"
    UVC = "uvc"
    UNITREE_WEBRTC = "unitree-webrtc"


class EncoderKind(str, Enum):
    AUTO = "auto"
    JETSON = "jetson"
    NVENC = "nvenc"
    VAAPI = "vaapi"
    X264 = "x264"


@dataclass(frozen=True)
class Endpoint:
    host: str
    port: int


@dataclass
class SourceConfig:
    kind: SourceKind = SourceKind.SYNTHETIC
    path: str = ""
    uvc_mode: str = "raw"
    unitree_ip: str = ""
    unitree_aes_key_env: str = "UNITREE_AES_128_KEY"
    unitree_aes_key_file: str = ""
    unitree_connect_timeout_s: float = 20.0
    unitree_signaling_timeout_s: float = 3.0
    unitree_frame_timeout_s: float = 3.0
    unitree_reconnect_delay_s: float = 1.0
    unitree_stop_timeout_s: float = 8.0

    def validate(self) -> None:
        if self.kind == SourceKind.FILE:
            path = Path(self.path)
            if not path.is_file():
                raise ValueError(f"video file does not exist: {path}")
        elif self.kind == SourceKind.UVC:
            if not self.path:
                self.path = "/dev/video0"
            if self.uvc_mode not in {"raw", "mjpeg"}:
                raise ValueError("uvc_mode must be raw or mjpeg")
        elif self.kind == SourceKind.UNITREE_WEBRTC:
            try:
                address = ipaddress.ip_address(self.unitree_ip)
            except ValueError as exc:
                raise ValueError("unitree_ip must be an IPv4 address literal") from exc
            if address.version != 4:
                raise ValueError("unitree_ip must be an IPv4 address literal")
            if not self.unitree_aes_key_env and not self.unitree_aes_key_file:
                raise ValueError("configure a Unitree AES key environment name or file path")
            if self.unitree_aes_key_file and not Path(self.unitree_aes_key_file).is_file():
                raise ValueError(
                    f"Unitree AES key file does not exist: {self.unitree_aes_key_file}"
                )
            timeouts = (
                self.unitree_connect_timeout_s,
                self.unitree_signaling_timeout_s,
                self.unitree_frame_timeout_s,
                self.unitree_reconnect_delay_s,
                self.unitree_stop_timeout_s,
            )
            if min(timeouts) <= 0.0:
                raise ValueError("Unitree WebRTC timeouts must be positive")


@dataclass
class StreamConfig:
    width: int = 1280
    height: int = 720
    fps: int = 30
    bitrate: int = 4_000_000
    gop_frames: int = 30
    encoder: EncoderKind = EncoderKind.AUTO
    queue_items: int = 4
    queue_bytes: int = 8 * 1024 * 1024
    queue_max_age_ms: int = 150

    def validate(self) -> None:
        if not 1 <= self.width <= 8192 or not 1 <= self.height <= 8192:
            raise ValueError("stream dimensions are out of range")
        if not 1 <= self.fps <= 240:
            raise ValueError("fps is out of range")
        if not 1_000 <= self.bitrate <= 200_000_000:
            raise ValueError("bitrate is out of range")
        if not 1 <= self.gop_frames <= 600:
            raise ValueError("gop_frames is out of range")
        if self.queue_items <= 0 or self.queue_bytes <= 0 or self.queue_max_age_ms <= 0:
            raise ValueError("queue limits must be positive")


@dataclass
class GatewayConfig:
    source: SourceConfig = field(default_factory=SourceConfig)
    stream: StreamConfig = field(default_factory=StreamConfig)
    control: Endpoint = field(default_factory=lambda: Endpoint("0.0.0.0", CONTROL_PORT))
    heartbeat: Endpoint = field(default_factory=lambda: Endpoint("0.0.0.0", HEARTBEAT_PORT))
    health: Endpoint = field(default_factory=lambda: Endpoint("127.0.0.1", HEALTH_PORT))
    video_port: int = VIDEO_PORT
    direct_target: Endpoint | None = None
    honor_open_camera_video_config: bool = True
    connect_timeout_s: float = 1.0
    send_timeout_s: float = 0.25
    reconnect_delay_s: float = 0.25
    send_buffer_bytes: int = 256 * 1024
    metrics_interval_s: float = 5.0
    heartbeat_stale_s: float = 2.0
    display_stale_s: float = 1.0

    def validate(self) -> None:
        self.source.validate()
        self.stream.validate()
        for name, endpoint in (("control", self.control), ("heartbeat", self.heartbeat), ("health", self.health)):
            if not endpoint.host or not 1 <= endpoint.port <= 65535:
                raise ValueError(f"invalid {name} endpoint: {endpoint}")
        if not 1 <= self.video_port <= 65535:
            raise ValueError("invalid default PICO video port")
        if self.direct_target is not None and not 1 <= self.direct_target.port <= 65535:
            raise ValueError("invalid direct target")
        if min(self.connect_timeout_s, self.send_timeout_s, self.reconnect_delay_s, self.metrics_interval_s) <= 0.0:
            raise ValueError("network and metrics timeouts must be positive")


def parse_endpoint(value: str) -> Endpoint:
    text = value.strip()
    if text.startswith("["):
        close = text.find("]")
        if close < 0 or close + 1 >= len(text) or text[close + 1] != ":":
            raise ValueError(f"endpoint must be [host]:port: {value}")
        host, port_text = text[1:close], text[close + 2 :]
    else:
        if ":" not in text:
            raise ValueError(f"endpoint must be host:port: {value}")
        host, port_text = text.rsplit(":", 1)
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(f"invalid endpoint port: {value}") from exc
    if not host or not 1 <= port <= 65535:
        raise ValueError(f"invalid endpoint: {value}")
    return Endpoint(host, port)
