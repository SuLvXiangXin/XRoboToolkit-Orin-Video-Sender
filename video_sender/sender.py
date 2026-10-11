"""One independent video listener, a capacity-one input, and isolated encoding."""
from dataclasses import dataclass
import json
import queue
import socket
import threading
import time
import uuid

import numpy as np

from .encoder import Encoder
from .protocol import CONFIG, VIDEO, STATUS, json_packet, packet, receive


@dataclass(frozen=True)
class Frame:
  data: bytes
  width: int
  height: int
  received: float
  source: str


class VideoSender:
  def __init__(self, host="0.0.0.0", port=13579, fps=30, bitrate=4000000,
               encoder="h264_nvenc", stale_s=.5):
    if not 1 <= fps <= 60 or not 64000 <= bitrate <= 50000000 or stale_s <= 0:
      raise ValueError("invalid video limits")
    if encoder not in ("h264_nvenc", "libx264"):
      raise ValueError("unsupported encoder")
    self.host, self.port = host, port
    self.fps, self.bitrate, self.encoder, self.stale_s = fps, bitrate, encoder, stale_s
    self._condition = threading.Condition()
    self._frame = None
    self._generation = 0
    self._stop = threading.Event()
    self._listener = None
    self._client = None
    self._thread = None
    self._metrics = {"submitted": 0, "overwritten": 0, "sent": 0, "connections": 0,
      "connected": False, "error": "", "encoder": encoder}

  def submit_frame(self, image, *, pixel_format="bgr", source="default", received_monotonic=None):
    if pixel_format not in ("bgr", "rgb") or image.dtype != np.uint8 or image.ndim != 3 or image.shape[2] != 3:
      raise ValueError("expected uint8 HxWx3 RGB/BGR image")
    height, width = image.shape[:2]
    if min(width, height) < 16 or max(width, height) > 4096 or width % 2 or height % 2:
      raise ValueError("H.264 requires even dimensions between 16 and 4096")
    if pixel_format == "rgb":
      image = image[..., ::-1]
    frame = Frame(np.ascontiguousarray(image).tobytes(), width, height,
      time.monotonic() if received_monotonic is None else received_monotonic, str(source))
    with self._condition:
      if self._frame is not None:
        self._metrics["overwritten"] += 1
      self._frame = frame
      self._metrics["submitted"] += 1
      self._metrics["last_source_monotonic"] = frame.received
      self._condition.notify_all()

  def reset_source(self):
    """Invalidate queued and in-flight video before an input adapter switches source."""
    with self._condition:
      self._generation += 1
      self._frame = None
      self._metrics.pop("last_source_monotonic", None)
      self._interrupt_client()
      self._condition.notify_all()

  def snapshot(self):
    with self._condition:
      result = dict(self._metrics)
    stamp = result.get("last_source_monotonic")
    result["source_age_ms"] = None if stamp is None else (time.monotonic() - stamp) * 1000
    result["source_fresh"] = stamp is not None and time.monotonic() - stamp <= self.stale_s
    status_time = result.get("receiver_status_monotonic", 0)
    result["receiver_status_fresh"] = result["connected"] and time.monotonic() - status_time < 2
    return result

  def _set(self, **values):
    with self._condition:
      self._metrics.update(values)

  def start(self):
    if self._thread is not None:
      raise RuntimeError("sender already started")
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
      listener.bind((self.host, self.port))
      listener.listen(1)
      listener.settimeout(.2)
    except BaseException:
      listener.close()
      raise
    self._listener = listener
    self.port = listener.getsockname()[1]
    self._thread = threading.Thread(target=self._run, name="video-session", daemon=True)
    self._thread.start()

  def _take(self, timeout=.1):
    with self._condition:
      if self._frame is None and not self._stop.is_set():
        self._condition.wait(timeout)
      frame, self._frame = self._frame, None
    if frame and time.monotonic() - frame.received <= self.stale_s:
      return frame
    return None

  def _run(self):
    while not self._stop.is_set():
      try:
        client, peer = self._listener.accept()
      except socket.timeout:
        continue
      except OSError:
        break
      with self._condition:
        self._client = client
        generation = self._generation
        self._metrics["connections"] += 1
      try:
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
        client.settimeout(.3)
        self._session(client, peer, generation)
      except (OSError, EOFError, ValueError, RuntimeError) as exc:
        self._set(error=f"{type(exc).__name__}: {exc}")
      finally:
        client.close()
        with self._condition:
          self._client = None
          self._metrics["connected"] = False

  def _session(self, client, peer, generation):
    # Client speaks first; this is a separate versioned protocol, never legacy pose/video traffic.
    client.settimeout(3)
    kind, payload = receive(client)
    hello = json.loads(payload)
    if kind != STATUS or not isinstance(hello, dict) or hello.get("protocol") != 1:
      raise ValueError("generic video v1 handshake required")
    frame = None
    deadline = time.monotonic() + 5
    while frame is None and not self._stop.is_set() and generation == self._generation:
      frame = self._take()
      if time.monotonic() > deadline:
        raise TimeoutError("no fresh source frame")
    if frame is None:
      return
    session = uuid.uuid4().hex
    config = {"protocol": 1, "session": session, "codec": "h264", "layout": "mono",
      "width": frame.width, "height": frame.height, "fps": self.fps,
      "bitrate": self.bitrate, "source": frame.source}
    client.sendall(json_packet(CONFIG, config))
    kind, payload = receive(client)
    ready = json.loads(payload)
    if kind != STATUS or not isinstance(ready, dict) or ready.get("ready") is not True or ready.get("session") != session:
      raise ValueError(f"receiver rejected stream: {ready}")
    client.settimeout(.3)
    self._set(connected=True, error="", encoder_error="", peer=peer[0], config=config,
      receiver={}, receiver_status_monotonic=0)
    codec = Encoder(frame.width, frame.height, self.fps, self.bitrate, self.encoder)
    done = threading.Event()
    failure = []

    def feed():
      current = frame
      last_source = current.received
      next_write = time.monotonic()
      try:
        while not done.is_set() and not self._stop.is_set() and generation == self._generation:
          if current is not None:
            if (current.width, current.height, current.source) != (frame.width, frame.height, frame.source):
              with self._condition:
                if self._frame is None:
                  self._frame = current
              raise RuntimeError("source format changed; renegotiate")
            last_source = current.received
            if done.wait(max(0, next_write - time.monotonic())):
              break
            if time.monotonic() - current.received <= self.stale_s:
              codec.write(current.data)
              next_write = max(next_write + 1 / self.fps, time.monotonic())
          if time.monotonic() - last_source > self.stale_s:
            raise TimeoutError("source frame stream stalled")
          current = self._take(.03)
      except (OSError, RuntimeError) as exc:
        failure.append(str(exc))
      finally:
        done.set()

    def status():
      try:
        while not done.is_set():
          # A timeout at any packet boundary ends the connection: never resume a partial frame.
          readable, _, _ = __import__('select').select([client], [], [], .1)
          if not readable:
            continue
          kind, payload = receive(client)
          value = json.loads(payload)
          if kind != STATUS or not isinstance(value, dict) or value.get("session") != session:
            raise ValueError("unexpected receiver status")
          self._set(receiver=value, receiver_status_monotonic=time.monotonic())
      except (OSError, EOFError, ValueError) as exc:
        failure.append(str(exc))
        done.set()

    writer = threading.Thread(target=feed, name="video-encode", daemon=True)
    reader = threading.Thread(target=status, name="video-status", daemon=True)
    writer.start()
    reader.start()
    try:
      while not done.is_set() and not self._stop.is_set() and generation == self._generation:
        if codec.failed.is_set():
          raise RuntimeError(codec.error or "encoder stopped")
        try:
          encoded_at, unit = codec.output.get(timeout=.05)
        except queue.Empty:
          continue
        if time.monotonic() - encoded_at > .15:
          raise TimeoutError("encoded queue too old; restart at keyframe")
        client.sendall(packet(VIDEO, unit))
        with self._condition:
          self._metrics["sent"] += 1
      if failure:
        raise RuntimeError(failure[0])
    finally:
      done.set()
      writer.join(timeout=2.2)
      reader.join(timeout=.5)
      error = codec.close()
      if error:
        self._set(encoder_error=error)

  def _interrupt_client(self):
    if self._client is not None:
      try:
        self._client.shutdown(socket.SHUT_RDWR)
      except OSError:
        pass

  def close(self):
    self._stop.set()
    with self._condition:
      self._interrupt_client()
      self._condition.notify_all()
    if self._listener is not None:
      self._listener.close()
    if self._thread is not None:
      self._thread.join(timeout=5)
      if self._thread.is_alive():
        raise RuntimeError("video worker did not stop")
