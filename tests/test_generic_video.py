import json
import socket
import struct
import threading
import time

import numpy as np
import pytest

from video_sender.encoder import AccessUnits
from video_sender.protocol import CONFIG, VIDEO, STATUS, packet, json_packet, receive
from video_sender.sender import VideoSender


def test_fragmentation_and_invalid_length():
  a, b = socket.socketpair()
  a.settimeout(1)
  wire = packet(VIDEO, b"hello")
  def send():
    for v in wire:
      b.sendall(bytes([v]))
  worker = threading.Thread(target=send)
  worker.start()
  assert receive(a) == (VIDEO, b"hello")
  worker.join()
  b.sendall(struct.pack("!BI", CONFIG, 20000))
  with pytest.raises(ValueError): receive(a)
  a.close(); b.close()


def test_access_units_across_arbitrary_reads():
  first = b"\x00\x00\x00\x01\x09\xf0\x00\x00\x01\x65first"
  second = b"\x00\x00\x01\x09\xf0\x00\x00\x01\x41second"
  p = AccessUnits(); units = []
  for v in first + second:
    units.extend(p.feed(bytes([v])))
  assert units == [first]


def test_latest_frame_owned_and_validated():
  sender = VideoSender()
  image = np.zeros((32, 32, 3), np.uint8)
  sender.submit_frame(image)
  image[:] = 42
  assert sender._take().data == bytes(32 * 32 * 3)
  sender.submit_frame(image); sender.submit_frame(image)
  assert sender.snapshot()["overwritten"] == 1
  with pytest.raises(ValueError): sender.submit_frame(image[:, :31])
  sender.reset_source()
  assert sender._take(timeout=0) is None
  sender.close()


def test_real_encoding_reconnect_and_resolution_change():
  # Synthetic input and an offline fake PICO; no camera, ROS, XR or robot SDK.
  sender = VideoSender(host="127.0.0.1", port=0, encoder="libx264")
  sender.start()
  stop = threading.Event()
  size = [64, 48]
  def frames():
    while not stop.is_set():
      w, h = size
      sender.submit_frame(np.zeros((h, w, 3), np.uint8))
      stop.wait(1 / 30)
  producer = threading.Thread(target=frames)
  producer.start()
  def connect(expected):
    client = socket.create_connection(("127.0.0.1", sender.port), timeout=5)
    client.sendall(json_packet(STATUS, {"protocol": 1}))
    kind, payload = receive(client)
    conf = json.loads(payload)
    assert kind == CONFIG and (conf["width"], conf["height"]) == expected
    client.sendall(json_packet(STATUS, {"ready": True, "session": conf["session"]}))
    kind, payload = receive(client)
    assert kind == VIDEO and b"\x00\x00\x00\x01\x09" in payload
    # First AU includes SPS/PPS/IDR: a new receiver can decode without old frames.
    import re
    nals = [m[0] & 31 for m in re.findall(b"\x00\x00(?:\x00)?\x01(.)", payload)]
    assert {7, 8, 5}.issubset(nals)
    return client
  try:
    # A malformed peer must not kill the listening worker.
    bad = socket.create_connection(("127.0.0.1", sender.port), timeout=5)
    bad.sendall(json_packet(STATUS, []))
    assert bad.recv(1) == b""
    bad.close()
    c = connect((64, 48)); c.close()
    time.sleep(.3)
    c = connect((64, 48)); c.close()
    sender.reset_source(); size[:] = [96, 64]
    time.sleep(.2)
    c = connect((96, 64)); c.close()
    c = connect((96, 64))
    # Stop the capture source. The session must end, not replay a frozen frame.
    stop.set(); producer.join()
    deadline = time.monotonic() + 3
    while True:
      try:
        receive(c)
      except EOFError:
        break
      assert time.monotonic() < deadline
    c.close()
  finally:
    stop.set(); producer.join(); sender.close()


def test_slow_receiver_disconnects_without_blocking_capture():
  sender = VideoSender(host="127.0.0.1", port=0, encoder="libx264", fps=60, bitrate=8000000)
  sender.start()
  stop = threading.Event()
  noise = np.random.default_rng(4).integers(0, 256, (256, 512, 3), dtype=np.uint8)
  def frames():
    while not stop.is_set():
      sender.submit_frame(noise)
      stop.wait(1 / 60)
  producer = threading.Thread(target=frames)
  producer.start()
  peer = socket.socket()
  peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
  peer.settimeout(3)
  try:
    peer.connect(("127.0.0.1", sender.port))
    peer.sendall(json_packet(STATUS, {"protocol": 1}))
    _, payload = receive(peer)
    config = json.loads(payload)
    peer.sendall(json_packet(STATUS, {"ready": True, "session": config["session"]}))
    before = sender.snapshot()["submitted"]
    deadline = time.monotonic() + 6
    # Intentionally do not read video. Sender must abandon the congested session.
    while not sender.snapshot()["error"] and time.monotonic() < deadline:
      time.sleep(.05)
    result = sender.snapshot()
    assert result["error"] and not result["connected"]
    assert result["submitted"] > before + 10
    assert sender._thread.is_alive()
  finally:
    peer.close(); stop.set(); producer.join(); sender.close()
