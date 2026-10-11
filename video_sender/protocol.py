"""Generic video v1: type:u8, length:u32 BE, payload. No pose SDK dependency."""
import json
import struct

CONFIG, VIDEO, STATUS = 1, 2, 3
MAX_PACKET = 16 * 1024 * 1024


def packet(kind, payload):
  if kind not in (CONFIG, VIDEO, STATUS) or not 0 < len(payload) <= MAX_PACKET:
    raise ValueError("invalid video packet")
  return struct.pack("!BI", kind, len(payload)) + payload


def json_packet(kind, value):
  return packet(kind, json.dumps(value, separators=(",", ":")).encode())


def read_exact(sock, size):
  data = bytearray()
  while len(data) < size:
    chunk = sock.recv(size - len(data))
    if not chunk:
      raise EOFError("video peer disconnected")
    data.extend(chunk)
  return bytes(data)


def receive(sock):
  kind, size = struct.unpack("!BI", read_exact(sock, 5))
  if kind not in (CONFIG, VIDEO, STATUS) or not 0 < size <= MAX_PACKET:
    raise ValueError("invalid packet header")
  if kind != VIDEO and size > 16384:
    raise ValueError("oversized control message")
  return kind, read_exact(sock, size)
