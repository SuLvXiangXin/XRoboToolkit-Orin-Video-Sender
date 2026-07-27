# Low-latency PICO Video Gateway

## Scope and isolation

The gateway has one job: capture/encode a camera source and send H.264 access
units to the existing XRoboToolkit Unity decoder. It is a separate OS process.
It does not use `RoboticsServiceProcess`, `xrobotoolkit_sdk`, LogosVLA, the WBC
WebSocket, DDS, or any robot action interface.

The official network directions remain:

```text
PICO Unity -> gateway TCP 13579       OPEN_CAMERA / CLOSE_CAMERA
gateway    -> PICO TCP 12345          4-byte BE length + H.264 access unit
PICO Unity -> gateway UDP 13580       optional receiver heartbeat v1
operator   -> gateway TCP 13581       localhost-only JSON health
```

Direct TCP requires bidirectional LAN reachability. Disable Wi-Fi client
isolation and allow only these ports on the trusted lab network. The protocol
is not authenticated or encrypted; do not expose it to an untrusted network.

## Runtime dependencies

The Python implementation uses the system GStreamer installation through
PyGObject so the camera/codec plugins remain platform-native:

```bash
sudo apt-get install \
  python3-gi gir1.2-gstreamer-1.0 \
  gstreamer1.0-tools gstreamer1.0-plugins-base \
  gstreamer1.0-plugins-good gstreamer1.0-plugins-bad \
  gstreamer1.0-plugins-ugly
```

Jetson images must additionally provide `nvvidconv` and
`nvv4l2h264enc`. An x86 host can use `nvh264enc`, `vah264enc` /
`vaapih264enc`, or `x264enc`. `--encoder auto` probes in that order and
actually starts each candidate; a present but unusable hardware plugin falls
through to the next candidate.

The optional Unitree source additionally uses the pinned sibling checkout:

```text
../unitree_webrtc_connect
version: 2.1.2
commit: c3e45a03ea62e5c664183b8848d035659d5c0fec
```

Install that checkout and its WebRTC runtime only on a host that needs this
source. The gateway passes `enable_audio=False`; the vendor keeps its historical
audio-enabled default, but lazily imports host audio support for video-only
callers.

Inspect the resolved candidates and pipeline without opening a camera/socket:

```bash
python3 -m vr_gateway --source synthetic --encoder auto --dry-run
```

## Sources

Synthetic test pattern with elapsed-time overlay:

```bash
python3 -m vr_gateway --source synthetic --listen 0.0.0.0:13579
```

Loop a video file:

```bash
python3 -m vr_gateway \
  --source file --source-path /absolute/path/test.mp4 \
  --listen 0.0.0.0:13579
```

UVC raw or MJPEG camera:

```bash
python3 -m vr_gateway \
  --source uvc --source-path /dev/video0 --uvc-mode raw \
  --listen 0.0.0.0:13579

python3 -m vr_gateway \
  --source uvc --source-path /dev/video0 --uvc-mode mjpeg \
  --listen 0.0.0.0:13579
```

## Optional Unitree WebRTC source (hardware pending)

This adapter targets the known H2 LAN camera endpoint whose expected legacy
signaling flow is `POST http://H2_IP:8081/offer`. The pinned vendor probes
`:9991` first for newer Unitree firmware and then falls back to legacy `:8081`.
That protocol support is present in software, but **no H2 camera, codec,
latency, reconnect, or thermal result has been verified yet**.

The gateway process is the sole Unitree WebRTC client. Starting it claims the
robot's one-client slot and keeps the asyncio/WebRTC peer in a dedicated worker
until the gateway stops. Close the official Unitree mobile app, browser viewers,
and any second SDK client before testing.

Probe reachability without sending an SDP offer:

```bash
export H2_IP=192.168.123.161
nc -vz -w 2 "$H2_IP" 8081
```

From the parent VR workspace, install/use the pinned video-only environment,
then run a camera-to-PICO trial:

```bash
cd /home/phymi/Project/whole_body_control/vr
./scripts/setup_video_gateway.sh
cd XRoboToolkit-Orin-Video-Sender

.venv/bin/python -m vr_gateway \
  --source unitree-webrtc \
  --unitree-ip "$H2_IP" \
  --encoder auto \
  --width 1280 --height 720 --fps 30 --bitrate 4000000 \
  --listen 0.0.0.0:13579
```

The locked core dependency list plus editable vendor `--no-deps` install is
intentional for this video-only service; it avoids installing
PortAudio/PyAudio/sounddevice. Use the vendor's normal installation path when
its audio features are also required.

Legacy `:8081/offer` normally needs no AES key. If the robot reports the newer
`data2=3` handshake, provide its per-device key through the default
`UNITREE_AES_128_KEY` environment variable or, preferably, a permission-limited
file:

```bash
chmod 600 /run/secrets/unitree_h2_aes
python3 -m vr_gateway \
  --source unitree-webrtc --unitree-ip "$H2_IP" \
  --unitree-aes-key-file /run/secrets/unitree_h2_aes \
  --encoder auto --listen 0.0.0.0:13579
```

There is deliberately no CLI option that accepts a literal AES key. Dry-run
JSON contains only the environment-variable name or file path, never resolved
key material.

The WebRTC worker decodes each video track frame to RGB at the configured
width/height and performs a non-blocking put into a capacity-one latest-frame
slot. A separate GStreamer `appsrc` worker consumes that slot. Both `appsrc`
and the existing pre-encoder raw queue are one-frame, downstream-leaky queues,
so a slow encoder drops old raw frames rather than backpressuring `track.recv()`.
The existing hardware encoder, GOP-safe encoded queue, and PICO transport remain
unchanged. The accepted Unity control peer remains the video destination, but
its width/height/FPS fields are ignored for this source so they cannot disagree
with the configured raw `appsrc` caps.

Connection failures are exposed through health/metrics with explicit codes:

- `robot_busy`: another WebRTC client owns the single slot.
- `aes_key_required` / `aes_key_rejected`: missing or wrong per-device key.
- `signaling_port_unreachable`: neither `:9991` nor `:8081` is reachable.
- `connect_timeout`, `frame_timeout`, or `track_ended`: bounded reconnect paths.

The LAN HTTP request, overall connect, frame freshness, reconnect delay, and
stop join all have explicit timeouts. Update them with the `--unitree-*-timeout-s`
and `--unitree-reconnect-delay-s` flags only after recording the original
failure and network conditions.

The source is selected locally. Unity's `camera` string is recorded as a
metric but cannot switch the gateway to an arbitrary local device or file.
Width, height, FPS, bitrate, and PICO port normally come from the validated
`OPEN_CAMERA` request. The PICO IP is the accepted TCP `13579` peer address,
not the payload's interface guess; a mismatch is recorded in health metrics.
Use `--ignore-open-camera-video-config` to keep the local video profile while
still using that peer destination.

For a direct diagnostic without Unity's `OPEN_CAMERA` control connection:

```bash
python3 -m vr_gateway \
  --source synthetic --encoder x264 \
  --target PICO_IP:12345
```

The initial implementation is H.264 only. An HEVC request is reported in
metrics and served as H.264; codec negotiation must be added explicitly before
using HEVC.

## Backpressure and recovery

The GStreamer capture path has a one-buffer `leaky=downstream` raw queue. If
encoding slows down, an old raw frame is discarded before encode. The appsink
callback only copies one encoded access unit into a bounded in-memory queue;
it never calls `connect`, `send`, or reconnect logic.

The encoded queue deliberately does **not** silently drop arbitrary P-frames.
If count, bytes, or age exceeds its configured bound, it flushes the complete
queued GOP, asks the encoder for a new keyframe, drops frames while waiting,
and resumes only when SPS, PPS, and IDR are available together. A network
timeout or reconnect uses the same recovery boundary, so old video is not
replayed after congestion.

Useful tuning flags:

```text
--queue-items 4
--queue-bytes 8388608
--queue-max-age-ms 150
--connect-timeout-s 1.0
--send-timeout-s 0.25
--reconnect-delay-s 0.25
```

## Metrics and receiver health

The gateway prints one aggregate JSON record every five seconds by default and
serves the current snapshot locally:

```bash
curl http://127.0.0.1:13581/healthz
```

Important fields include capture/encode/send counters, queue depth/bytes,
recovery requests, reconnect/errors, active encoder, pipeline state, and
receiver heartbeat/display-progress ages.

For the Unitree source, also inspect `unitree_source_state`,
`unitree_last_error_code`, `unitree_frame_age_ms`, raw overwrite/stale-drop
counters, and `source_healthy`. These prove software state and freshness only;
they do not constitute H2 hardware validation.

Sender health is not proof that the operator sees a fresh frame. The optional
receiver heartbeat separately advances `displayed_frames`; repeated
heartbeats without display progress become unhealthy. `visual_feedback_healthy`
also requires Unity's explicit `visual_profile_available=true`; the bundled
observation-only profile therefore stays false even with smooth decoded video.
Until both conditions exist it remains false. Do not enable a visual-dependent
robot safety profile from sender-side metrics alone.

See [UNITY_RECEIVER_HEARTBEAT.md](UNITY_RECEIVER_HEARTBEAT.md) for the datagram
schema. The listener is independent and does not forward health through PC
Service or WBC.

## Validation

All default tests require no camera and no PICO. They cover official protocol
framing, fragmented/coalesced control input, H.264 recovery construction,
bounded GOP-safe queue behavior, timeout/reconnect, configuration/encoder
selection, receiver heartbeat freshness, a synthetic GStreamer pipeline, raw
queue backpressure, fake Unitree tracks, Unitree `appsrc`, secret-redaction,
and a local fake-PICO end-to-end stream:

```bash
python3 -m unittest discover -s tests -v
```

Hardware validation remains separate: build/run on the target Jetson or x86
host, then test the real PICO decoder, start/stop cycles, cable/Wi-Fi loss,
latency, thermal behavior, and long soak.
