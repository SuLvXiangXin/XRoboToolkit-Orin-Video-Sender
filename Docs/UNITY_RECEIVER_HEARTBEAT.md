# Unity receiver heartbeat v1

The gateway listens for optional UDP JSON datagrams on port `13580`. This is a
small receiver-health plane, not a video transport and not part of XRoboToolkit
PC Service. The Unity client sends best-effort, non-blocking datagrams at 2 Hz
by default.

The canonical Unity v1 payload is:

```json
{
  "schema": "xrobo_receiver_heartbeat_v1",
  "version": 1,
  "session_id": "session-a",
  "sequence": 42,
  "sent_unix_ns": 123456789,
  "source": "CAMERA_ONLY",
  "visual_profile": "camera_flat_head_follow_v1",
  "display_mode": "flat",
  "follow_head": true,
  "receiver_state": "streaming",
  "connection_state": "streaming_inferred",
  "decoder_state": "playing",
  "receive_observation": "decoder_input_queued",
  "raw_receive_observable": false,
  "last_receive_age_ms": 10,
  "last_decoded_age_ms": 11,
  "last_texture_update_age_ms": 12,
  "receive_fps": 29.9,
  "decoded_fps": 29.8,
  "texture_update_fps": 29.7,
  "unity_update_hz": 89.9,
  "decoded_frames": 100,
  "displayed_frames": 99,
  "visual_profile_available": true,
  "heartbeat_timeout_ms": 2000,
  "publisher_dropped_count": 3
}
```

The exact serializer output is checked into
`tests/fixtures/unity_receiver_heartbeat_v1.json` and is consumed by both the
heartbeat decoder test and the local fake-PICO end-to-end test. The gateway
requires `version`, `session_id`, `sequence`, `displayed_frames`,
`decoded_frames`, `unity_update_hz`, and `decoder_state`; the remaining Unity
fields are forward-compatible diagnostic context.

Rules:

- `session_id` changes on app/decoder session restart.
- `sequence` strictly increases within a session. Replays do not refresh
  heartbeat freshness.
- `displayed_frames` advances only after Unity successfully calls the decoder
  texture update. A connected socket or received byte count is not display
  progress or proof that a photon reached the display.
- `decoded_frames` is the decoder output counter.
- `decoder_state` is `playing` only while decoded frames are being displayed;
  other useful values are `starting`, `stalled`, `disconnected`, and `error`.
- Datagrams larger than 4096 bytes, malformed JSON, unknown versions, invalid
  counters, or non-finite rates are rejected.

The aggregate health exposes two independent ages:

- `heartbeat_age_s`: last accepted heartbeat.
- `display_progress_age_s`: last increase of `displayed_frames`.

`receiver_healthy` requires both ages to be below configured thresholds and
`decoder_state == "playing"`. This is display freshness only.
`visual_feedback_healthy` additionally requires
`receiver_visual_profile_available == true`; a missing field defaults false,
and the bundled observation-only Unity profile explicitly reports false. A
safety supervisor may read this state from the localhost health endpoint, but
video recovery must never automatically acquire robot control.
