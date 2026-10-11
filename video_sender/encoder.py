"""Isolated FFmpeg encoder. Backpressure aborts the GOP instead of dropping P frames."""
import os
import queue
import re
import select
import subprocess
import tempfile
import threading
import time

AUD = re.compile(b"\x00\x00(?:\x00)?\x01\x09")


class AccessUnits:
  def __init__(self):
    self.data = bytearray()

  def feed(self, data):
    self.data.extend(data)
    starts = [m.start() for m in AUD.finditer(self.data)]
    units = [bytes(self.data[a:b]) for a, b in zip(starts, starts[1:])]
    if len(starts) > 1:
      del self.data[:starts[-1]]
    if len(self.data) > 16 * 1024 * 1024:
      raise ValueError("encoder access unit exceeds limit")
    return units


def command(width, height, fps, bitrate, encoder):
  if encoder not in ("h264_nvenc", "libx264"):
    raise ValueError("encoder must be h264_nvenc or libx264")
  options = (["-preset", "p1", "-tune", "ull", "-rc", "cbr", "-zerolatency", "1", "-delay", "0"]
    if encoder == "h264_nvenc" else ["-preset", "ultrafast", "-tune", "zerolatency"])
  return ["ffmpeg", "-hide_banner", "-loglevel", "error", "-filter_threads", "1", "-f", "rawvideo",
    "-pixel_format", "bgr24", "-video_size", f"{width}x{height}", "-framerate", str(fps),
    "-i", "pipe:0", "-an", "-c:v", encoder, *options, "-pix_fmt", "yuv420p",
    "-b:v", str(bitrate), "-maxrate", str(bitrate), "-bufsize", str(bitrate // 2),
    "-g", str(fps), "-bf", "0", "-threads", "2", "-bsf:v", "h264_metadata=aud=insert",
    "-flush_packets", "1", "-f", "h264", "pipe:1"]


class Encoder:
  def __init__(self, width, height, fps, bitrate, encoder):
    self.output = queue.Queue(maxsize=3)
    self.failed = threading.Event()
    self.ready = threading.Event()
    self.error = ""
    self.stderr = tempfile.TemporaryFile()
    self.process = subprocess.Popen(command(width, height, fps, bitrate, encoder),
      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr, bufsize=0)
    os.set_blocking(self.process.stdin.fileno(), False)
    self.thread = threading.Thread(target=self._read, name="h264-output", daemon=True)
    self.thread.start()

  def _read(self):
    parser = AccessUnits()
    try:
      while data := os.read(self.process.stdout.fileno(), 65536):
        for unit in parser.feed(data):
          self.ready.set()
          self.output.put_nowait((time.monotonic(), unit))
    except (OSError, ValueError, queue.Full) as exc:
      self.error = f"encoder queue/stream failure: {type(exc).__name__}: {exc}"
    finally:
      self.failed.set()

  def write(self, data):
    view = memoryview(data)
    # GPU context creation is slower than steady-state encoding. Only startup
    # gets this allowance; encoded/network queues remain bounded throughout.
    deadline = time.monotonic() + (.3 if self.ready.is_set() else 2)
    while view:
      remaining = deadline - time.monotonic()
      if self.failed.is_set() or remaining <= 0:
        raise TimeoutError(self.error or "encoder input stalled")
      if not select.select([], [self.process.stdin], [], remaining)[1]:
        raise TimeoutError("encoder input stalled")
      try:
        count = os.write(self.process.stdin.fileno(), view)
        view = view[count:]
      except BlockingIOError:
        continue

  def close(self):
    if self.process.poll() is None:
      self.process.terminate()
      try:
        self.process.wait(timeout=1)
      except subprocess.TimeoutExpired:
        self.process.kill()
        self.process.wait(timeout=1)
    self.thread.join(timeout=1)
    self.stderr.seek(0)
    error = self.stderr.read(4096).decode(errors="replace")
    self.process.stdin.close()
    self.process.stdout.close()
    self.stderr.close()
    return error
