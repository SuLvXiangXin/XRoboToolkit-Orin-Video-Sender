# XRoboToolkit-Orin-Video-Sender
Video Previewer/Encoder/Sender on Nvidia Jetson Orin Platform

## Independent low-latency Video Gateway

`vr_gateway` is the maintained camera-to-PICO path in this branch. It keeps the
official Unity wire interfaces while isolating capture/encode/network work from
XR tracking and robot control:

- Unity camera control: TCP `13579`, including `OPEN_CAMERA`/`CLOSE_CAMERA`.
- PICO encoded-video ingress: TCP `12345` by default.
- Video framing: `4-byte big-endian access-unit length + H.264 access unit`.
- Sources: synthetic test pattern, video file, UVC (`raw` or `mjpeg`), and an
  optional Unitree WebRTC video source.
- Encoders: Jetson `nvv4l2h264enc`, x86 NVENC, VAAPI, then x264 fallback.
- Raw capture handoff: one-frame leaky/latest queue before the encoder.
- Encoded/network handoff: bounded, GOP-aware queue. Congestion flushes the
  old GOP and resumes only from an SPS/PPS/IDR recovery access unit.
- Network: a dedicated worker with `TCP_NODELAY`, bounded send buffer, connect
  and send timeouts, reconnect, and bounded shutdown.
- Receiver health: independent UDP heartbeat listener on `13580` and aggregate
  JSON health on `127.0.0.1:13581`.

It does not import or communicate with XRoboToolkit PC Service, LogosVLA, WBC,
or a robot action API. See [Docs/VIDEO_GATEWAY.md](Docs/VIDEO_GATEWAY.md) for
installation, commands, ports, health semantics, and tests.

Quick camera-free check:

```bash
make gateway-test
python3 -m vr_gateway --source synthetic --encoder auto --dry-run
```

Start the normal Unity-controlled synthetic stream:

```bash
python3 -m vr_gateway \
  --source synthetic \
  --listen 0.0.0.0:13579 \
  --width 1280 --height 720 --fps 30 --bitrate 4000000
```

In the Unity app, request the camera from this machine's LAN address. The
gateway uses the accepted control connection's peer address as the
authoritative PICO destination and connects back to its video listener on port
`12345`; the payload IP is retained only as a mismatch diagnostic.

The `unitree-webrtc` adapter is software-tested with fake tracks and GStreamer
`appsrc`, but has **not** been validated on the H2 hardware in this workspace.
It uses the pinned sibling `unitree_webrtc_connect` checkout as an optional
dependency and keeps the gateway as the robot's only WebRTC client. See the
hardware probe and launch procedure in
[Docs/VIDEO_GATEWAY.md](Docs/VIDEO_GATEWAY.md#optional-unitree-webrtc-source-hardware-pending).

![Screenshot](Docs/screenshot.png)
> Sender (Webcam): `./OrinVideoSender --preview --send --server 192.168.1.176 --port 12345`

> Receiver (Video-Viewer): TCP - 192.168.1.176 - 12345 - 1280x720

## Features

- Support Webcam and ZED cameras
- Preview
- H264 Encoding (via GStreamer)
- TCP/UDP sending w/ and w/o ASIO


## How to

- Setup necessary environment on Orin

- Build
```
# Update `Makefile` to choose the protocol [TCP/UDP], camera type [Webcam/ZED], w/ or w/o ASIO.
# Default: TCP w/o asio.

# install zmq
sudo apt-get install libzmq3-dev

make

./OrinVideoSender --help

# Listen to coming command from VR, 192.168.1.153 is the Orin IP address
# Add `--preview` to show the video on Orin if necessary 
./OrinVideoSender --listen 192.168.1.153:13579

# send the video stream to both VR via TCP and my own ubuntu via ZMQ
./OrinVideoSender --listen 192.168.1.153:13579 --zmq tcp://*:5555

# Direct send the video stream # 192.168.1.176 is the VR headset IP
# Add `--preview` to show the video on Orin if necessary 
./OrinVideoSender --send --server 192.168.1.176 --port 12345
```

## One More Thing 

- For software encoding ffmpeg, please refer to [RobotVisionTest](https://github.com/XR-Robotics/RobotVision-PC/tree/main/VideoTransferPC/RobotVisionTest).

> Note: Hardware ffmpeg encoding is not availalbe yet.

> Note: Jetson Multimedia API is not in use yet.

- For encoded h264 stream receiver, please refer to [VideoPlayer](https://github.com/XR-Robotics/RobotVision-PC/tree/main/VideoTransferPC/VideoPlayer) [TCP Only].

- For a general video player, please refer to [Video-Viewer](https://github.com/XR-Robotics/XRoboToolkit-Native-Video-Viewer) [TCP/UDP].

- The encoded h264 stream can be also played in [Unity-Client](https://github.com/XR-Robotics/XRoboToolkit-Unity-Client) [TCP Only].
