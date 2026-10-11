# Generic mono video v1

This optional Python sender extends the official source at
`8bedc7bb225628235f97b6e51bfde5a2a3704031`. Existing C++ camera modes are unchanged.
It requires the matching GenericVideoView/GenericVideoReceiver Unity extension.
It does not call PC Service, the pose SDK, or any robot command interface.

```sh
uv run xrobotoolkit-video-sender --encoder libx264 --width 928 --height 800
uv build
uv run --group dev python -m pytest -q tests/test_generic_video.py
```

The CLI generates a test pattern. Applications submit BGR/RGB uint8 NumPy arrays
without going through ROS:

```python
from video_sender import VideoSender
sender = VideoSender(host="0.0.0.0", port=13579, encoder="h264_nvenc")
sender.start()
try:
  sender.submit_frame(image, pixel_format="bgr", source="head-left")
  # Continue submitting from your capture loop. Each call replaces the pending frame.
finally:
  sender.close()
```

The caller owns its capture loop and calls `reset_source()` before replacing an
input adapter. Format/source changes also trigger negotiation automatically.
The library owns a copy of each submitted frame; there is only one pending frame.
Input requires even dimensions, 16–4096 per axis. H.264 hardware support may impose
additional size/rate limits, reported by the receiver. No fixed camera presets,
implicit resize, stereo calibration, or camera-specific geometry are used.

FFmpeg runs separately, with two codec threads, no B frames, explicit AUDs and a
one-second GOP. `h264_nvenc` requires a compatible workstation GPU/FFmpeg build;
Jetson availability is not implied. `libx264` is an explicit CPU alternative.
First GPU initialization may take up to two seconds; steady-state input writes
time out after 300 ms. Encoded output has at most three access units, 150 ms queue
age, and 300 ms socket timeout. Congestion closes the session instead of dropping
reference frames. Source age >500 ms also ends it. Receiver reconnects at an IDR.

## Wire format

Independent TCP connection, default port 13579. PICO connects to the sender;
the same connection works over `adb reverse tcp:23579 tcp:13579` with device host
127.0.0.1 and port 23579 (device port 13579 is used by the legacy control server).
Trusted network/USB only; authentication and encryption are not provided.

Each packet is `uint8 type | uint32 big-endian length | payload`. Types:

| Type | Payload | Limit |
| --- | --- | --- |
| 1 CONFIG | UTF-8 JSON | 16 KiB |
| 2 VIDEO | One complete Annex B H.264 access unit | 16 MiB |
| 3 STATUS | UTF-8 JSON | 16 KiB |

Client sends STATUS `{"protocol":1}`. Sender replies CONFIG with `protocol:1`,
`session` (new ID), `codec:"h264"`, `layout:"mono"`, `width`, `height`, `fps`,
`bitrate`, `source`. Client configures its surface/hardware decoder and replies
STATUS `{"session":"...","ready":true}`, or false with `error`.
Only then does encoding start. The first access unit includes SPS/PPS/IDR.
Client sends periodic STATUS with the same session and receiver counters.
Changing format requires a new connection/session; partial packet reads are
assembled, and EOF/invalid lengths terminate the connection. This is not the
legacy C++ sender's control/video protocol.

Receiver counters report presentation freshness and Unity frame intervals, not
camera-to-display latency. Tracking independence must be measured on the actual
headset with video off/on; separate workers do not eliminate shared GPU/CPU costs.
