from __future__ import annotations

from dataclasses import dataclass
import struct


CONTROL_PORT = 13579
VIDEO_PORT = 12345
HEARTBEAT_PORT = 13580
HEALTH_PORT = 13581
MAX_CONTROL_BODY_BYTES = 64 * 1024
MAX_ACCESS_UNIT_BYTES = 16 * 1024 * 1024


class ProtocolError(ValueError):
    pass


@dataclass(frozen=True)
class ControlCommand:
    name: str
    data: bytes


@dataclass(frozen=True)
class CameraRequest:
    width: int
    height: int
    fps: int
    bitrate: int
    enable_mv_hevc: int
    render_mode: int
    port: int
    camera: str
    ip: str

    @classmethod
    def decode(cls, payload: bytes) -> "CameraRequest":
        if len(payload) < 33:
            raise ProtocolError("camera request is truncated")
        if payload[:2] != b"\xca\xfe":
            raise ProtocolError("camera request magic must be CA FE")
        if payload[2] != 1:
            raise ProtocolError(f"unsupported camera request version: {payload[2]}")

        values = struct.unpack_from("<7i", payload, 3)
        offset = 31
        camera, offset = _decode_compact_string(payload, offset)
        ip, offset = _decode_compact_string(payload, offset)
        if offset != len(payload):
            raise ProtocolError("camera request contains trailing bytes")

        request = cls(
            width=values[0],
            height=values[1],
            fps=values[2],
            bitrate=values[3],
            enable_mv_hevc=values[4],
            render_mode=values[5],
            port=values[6],
            camera=camera,
            ip=ip,
        )
        request.validate()
        return request

    def validate(self) -> None:
        if not 1 <= self.width <= 8192 or not 1 <= self.height <= 8192:
            raise ProtocolError(f"invalid camera dimensions: {self.width}x{self.height}")
        if not 1 <= self.fps <= 240:
            raise ProtocolError(f"invalid camera fps: {self.fps}")
        if not 1_000 <= self.bitrate <= 200_000_000:
            raise ProtocolError(f"invalid camera bitrate: {self.bitrate}")
        if not 1 <= self.port <= 65535:
            raise ProtocolError(f"invalid PICO video port: {self.port}")
        if not self.ip or len(self.ip) > 255:
            raise ProtocolError("camera request has an invalid PICO address")


class ControlFrameDecoder:
    """Incrementally decode the official 4-byte big-endian outer framing."""

    def __init__(self, max_body_bytes: int = MAX_CONTROL_BODY_BYTES) -> None:
        self._max_body_bytes = int(max_body_bytes)
        self._buffer = bytearray()
        self._expected: int | None = None

    def feed(self, data: bytes) -> list[bytes]:
        if data:
            self._buffer.extend(data)
        frames: list[bytes] = []
        while True:
            if self._expected is None:
                if len(self._buffer) < 4:
                    break
                self._expected = struct.unpack_from(">I", self._buffer)[0]
                del self._buffer[:4]
                if self._expected < 8:
                    raise ProtocolError(f"control frame is too small: {self._expected}")
                if self._expected > self._max_body_bytes:
                    raise ProtocolError(f"control frame exceeds {self._max_body_bytes} bytes")
            if len(self._buffer) < self._expected:
                break
            frames.append(bytes(self._buffer[: self._expected]))
            del self._buffer[: self._expected]
            self._expected = None
        return frames


def decode_control_command(body: bytes) -> ControlCommand:
    if len(body) < 8:
        raise ProtocolError("control command is truncated")
    command_length = struct.unpack_from("<i", body, 0)[0]
    if command_length < 0 or command_length > 256:
        raise ProtocolError(f"invalid command length: {command_length}")
    data_length_offset = 4 + command_length
    if data_length_offset + 4 > len(body):
        raise ProtocolError("control command name is truncated")
    command_bytes = body[4:data_length_offset]
    data_length = struct.unpack_from("<i", body, data_length_offset)[0]
    if data_length < 0:
        raise ProtocolError(f"invalid command payload length: {data_length}")
    payload_offset = data_length_offset + 4
    if payload_offset + data_length != len(body):
        raise ProtocolError("control command payload length does not match frame")
    try:
        name = command_bytes.split(b"\0", 1)[0].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolError("control command is not valid UTF-8") from exc
    if not name:
        raise ProtocolError("control command name is empty")
    return ControlCommand(name=name, data=bytes(body[payload_offset:]))


def encode_control_command(name: str, data: bytes) -> bytes:
    command = name.encode("utf-8")
    body = struct.pack("<i", len(command)) + command + struct.pack("<i", len(data)) + data
    return struct.pack(">I", len(body)) + body


def encode_camera_request(request: CameraRequest) -> bytes:
    request.validate()
    fixed = b"\xca\xfe\x01" + struct.pack(
        "<7i",
        request.width,
        request.height,
        request.fps,
        request.bitrate,
        request.enable_mv_hevc,
        request.render_mode,
        request.port,
    )
    return fixed + _encode_compact_string(request.camera) + _encode_compact_string(request.ip)


def pack_video_access_unit(payload: bytes) -> bytes:
    size = len(payload)
    if size <= 0 or size > MAX_ACCESS_UNIT_BYTES:
        raise ProtocolError(f"encoded access unit size is invalid: {size}")
    return struct.pack(">I", size) + payload


def unpack_video_access_unit(packet: bytes) -> bytes:
    if len(packet) < 4:
        raise ProtocolError("video packet is truncated")
    size = struct.unpack_from(">I", packet)[0]
    if size != len(packet) - 4:
        raise ProtocolError("video packet length does not match payload")
    if size > MAX_ACCESS_UNIT_BYTES:
        raise ProtocolError("video packet is too large")
    return bytes(packet[4:])


def _decode_compact_string(payload: bytes, offset: int) -> tuple[str, int]:
    if offset >= len(payload):
        raise ProtocolError("compact string length is missing")
    length = payload[offset]
    offset += 1
    end = offset + length
    if end > len(payload):
        raise ProtocolError("compact string is truncated")
    try:
        return payload[offset:end].decode("utf-8"), end
    except UnicodeDecodeError as exc:
        raise ProtocolError("compact string is not valid UTF-8") from exc


def _encode_compact_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) > 255:
        raise ProtocolError("compact string exceeds 255 bytes")
    return bytes((len(encoded),)) + encoded
