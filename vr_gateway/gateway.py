from __future__ import annotations

from dataclasses import replace
import json
import threading
import time
from typing import Any

from .config import EncoderKind, GatewayConfig, SourceKind, StreamConfig
from .control import ControlServer
from .health_api import HealthServer
from .heartbeat import HeartbeatListener, ReceiverHealth
from .metrics import Metrics
from .pipeline import GstCapturePipeline, available_gstreamer_features, encoder_candidates
from .protocol import CameraRequest
from .queueing import AccessUnitQueue
from .transport import Destination, VideoTransport
from .unitree_source import LatestRawFrameQueue, UnitreeWebRTCSource


class VideoGateway:
    def __init__(self, config: GatewayConfig) -> None:
        config.validate()
        self.config = config
        self.metrics = Metrics()
        self.receiver_health = ReceiverHealth(
            heartbeat_stale_s=config.heartbeat_stale_s,
            display_stale_s=config.display_stale_s,
        )
        self.queue = AccessUnitQueue(
            max_items=config.stream.queue_items,
            max_bytes=config.stream.queue_bytes,
            max_age_s=config.stream.queue_max_age_ms / 1000.0,
        )
        self.transport = VideoTransport(
            self.queue,
            request_recovery=self.request_keyframe,
            metrics=self.metrics,
            reconnect_delay_s=config.reconnect_delay_s,
            connect_timeout_s=config.connect_timeout_s,
            send_timeout_s=config.send_timeout_s,
            send_buffer_bytes=config.send_buffer_bytes,
        )
        self.control = ControlServer(
            config.control,
            on_open=self.open_from_unity,
            on_close=self.close_stream,
            metrics=self.metrics,
        )
        self.heartbeat = HeartbeatListener(config.heartbeat, self.receiver_health, self.metrics)
        self.health_server = HealthServer(config.health, self.health_snapshot)
        self._lock = threading.RLock()
        self._pipeline: GstCapturePipeline | None = None
        self._active_stream: StreamConfig | None = None
        self._active_target: Destination | None = None
        self._pipeline_error = threading.Event()
        self._stop = threading.Event()
        self._monitor_thread: threading.Thread | None = None
        self._metrics_thread: threading.Thread | None = None
        self._unitree_frames: LatestRawFrameQueue | None = None
        self._unitree_source: UnitreeWebRTCSource | None = None
        if config.source.kind == SourceKind.UNITREE_WEBRTC:
            self._unitree_frames = LatestRawFrameQueue()
            self._unitree_source = UnitreeWebRTCSource(
                config.source,
                self._unitree_frames,
                self.metrics,
                width=config.stream.width,
                height=config.stream.height,
            )

    def start(self) -> None:
        self._stop.clear()
        try:
            if self._unitree_source is not None:
                self._unitree_source.start()
            self.transport.start()
            self.heartbeat.start()
            self.health_server.start()
            self.control.start()
            self._monitor_thread = threading.Thread(target=self._monitor, name="gateway-monitor", daemon=True)
            self._monitor_thread.start()
            self._metrics_thread = threading.Thread(target=self._report_metrics, name="gateway-metrics", daemon=True)
            self._metrics_thread.start()
            if self.config.direct_target is not None:
                self.open_stream(
                    Destination(self.config.direct_target.host, self.config.direct_target.port),
                    self.config.stream,
                )
        except Exception:
            self.stop()
            raise

    def stop(self) -> None:
        self._stop.set()
        self._pipeline_error.set()
        self.control.stop()
        self.close_stream()
        self.transport.stop()
        if self._unitree_source is not None:
            self._unitree_source.stop()
        if self._unitree_frames is not None:
            self._unitree_frames.close()
        self.heartbeat.stop()
        self.health_server.stop()
        for thread_name in ("_monitor_thread", "_metrics_thread"):
            thread = getattr(self, thread_name)
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=1.0)
            setattr(self, thread_name, None)

    def open_from_unity(self, request: CameraRequest) -> None:
        stream = self.config.stream
        if (
            self.config.honor_open_camera_video_config
            and self.config.source.kind != SourceKind.UNITREE_WEBRTC
        ):
            stream = replace(
                stream,
                width=request.width,
                height=request.height,
                fps=request.fps,
                bitrate=request.bitrate,
            )
            stream.validate()
        elif (
            self.config.honor_open_camera_video_config
            and self.config.source.kind == SourceKind.UNITREE_WEBRTC
        ):
            self.metrics.inc("unitree_open_camera_video_config_ignored")
        if request.enable_mv_hevc:
            self.metrics.inc("hevc_requests_ignored")
            self.metrics.set("last_warning", "HEVC requested; gateway currently sends H.264")
        self.metrics.set("unity_camera_name", request.camera)
        self.open_stream(Destination(request.ip, request.port), stream)

    def open_stream(self, target: Destination, stream: StreamConfig) -> None:
        with self._lock:
            needs_restart = self._pipeline is None or self._active_stream != stream
            if needs_restart:
                self._active_target = None
                self._active_stream = None
                self.receiver_health.reset()
                self.transport.clear_target()
                self._stop_pipeline_locked()
                try:
                    self._start_pipeline_locked(stream)
                except Exception:
                    self._stop_pipeline_locked()
                    raise
            self._active_target = target
            self._active_stream = replace(stream)
            self.receiver_health.reset(target.host)
            self.transport.set_target(target)
            self.metrics.set("video_target", f"{target.host}:{target.port}")

    def close_stream(self) -> None:
        with self._lock:
            self._active_target = None
            self._active_stream = None
            self.receiver_health.reset()
            self.transport.clear_target()
            self._stop_pipeline_locked()
            self.metrics.set("video_target", None)

    def request_keyframe(self) -> None:
        with self._lock:
            pipeline = self._pipeline
        if pipeline is not None:
            pipeline.request_keyframe()

    def health_snapshot(self) -> dict[str, Any]:
        queue = self.queue.snapshot()
        if self._unitree_frames is not None:
            queue.update(self._unitree_frames.snapshot())
        receiver = self.receiver_health.snapshot()
        snapshot = self.metrics.snapshot({**queue, **receiver})
        now_ns = time.monotonic_ns()
        for timestamp_name, age_name in (
            ("last_capture_monotonic_ns", "capture_age_ms"),
            ("last_send_monotonic_ns", "send_age_ms"),
            ("last_unitree_frame_monotonic_ns", "unitree_frame_age_ms"),
            ("last_unitree_appsrc_monotonic_ns", "unitree_appsrc_age_ms"),
        ):
            timestamp = snapshot.get(timestamp_name)
            snapshot[age_name] = (
                None if timestamp is None else max(0.0, (now_ns - int(timestamp)) * 1e-6)
            )
        with self._lock:
            stream_active = self._active_target is not None
        snapshot["stream_active"] = stream_active
        source_healthy = True
        if self.config.source.kind == SourceKind.UNITREE_WEBRTC:
            source_healthy = bool(
                snapshot.get("unitree_source_state") == "streaming"
                and snapshot.get("unitree_frame_age_ms") is not None
                and float(snapshot["unitree_frame_age_ms"])
                <= self.config.source.unitree_frame_timeout_s * 1000.0
            )
        snapshot["source_healthy"] = source_healthy
        snapshot["sender_healthy"] = bool(
            source_healthy
            and snapshot.get("pipeline_running")
            and snapshot.get("video_connected")
        )
        snapshot["visual_feedback_healthy"] = bool(
            stream_active
            and receiver["receiver_healthy"]
            and receiver["receiver_visual_profile_available"]
        )
        return snapshot

    def _start_pipeline_locked(self, stream: StreamConfig) -> None:
        features = available_gstreamer_features()
        candidates = encoder_candidates(stream.encoder, features)
        if not candidates:
            raise RuntimeError(
                f"no supported H.264 encoder for {stream.encoder.value}; available={sorted(features)}"
            )
        errors: list[str] = []
        for candidate in candidates:
            self.queue.require_recovery()
            pipeline = GstCapturePipeline(
                stream,
                self.config.source,
                candidate,
                self.queue,
                self.metrics,
                error_callback=self._on_pipeline_error,
                raw_frame_source=self._unitree_frames,
            )
            try:
                pipeline.start()
                self._pipeline = pipeline
                self.metrics.inc("pipeline_starts")
                return
            except Exception as exc:
                pipeline.stop()
                self.queue.require_recovery()
                errors.append(f"{candidate.value}: {exc}")
                self.metrics.inc(f"encoder_start_failures_{candidate.value}")
                self.metrics.set(f"encoder_start_error_{candidate.value}", str(exc))
                if stream.encoder != EncoderKind.AUTO:
                    break
        raise RuntimeError("all encoder candidates failed: " + "; ".join(errors))

    def _stop_pipeline_locked(self) -> None:
        pipeline, self._pipeline = self._pipeline, None
        if pipeline is not None:
            pipeline.stop()
            self.metrics.inc("pipeline_stops")
        self.queue.require_recovery()

    def _on_pipeline_error(self, message: str) -> None:
        self.metrics.set("pipeline_last_error", message)
        self._pipeline_error.set()

    def _monitor(self) -> None:
        while not self._stop.is_set():
            if not self._pipeline_error.wait(0.2):
                continue
            self._pipeline_error.clear()
            if self._stop.is_set():
                break
            with self._lock:
                target = self._active_target
                stream = self._active_stream
                if target is None or stream is None:
                    continue
                self.transport.clear_target()
                self._stop_pipeline_locked()
            if self._stop.wait(self.config.reconnect_delay_s):
                break
            try:
                with self._lock:
                    if target != self._active_target or stream != self._active_stream:
                        continue
                    self._start_pipeline_locked(stream)
                    self.transport.set_target(target)
                    self.metrics.inc("pipeline_restarts")
            except Exception as exc:
                self.metrics.inc("pipeline_restart_errors")
                self.metrics.set("pipeline_last_error", str(exc))
                self._pipeline_error.set()

    def _report_metrics(self) -> None:
        last_time = time.monotonic()
        last_values: dict[str, int] = {}
        while not self._stop.wait(self.config.metrics_interval_s):
            now = time.monotonic()
            elapsed = max(now - last_time, 1e-9)
            current = self.metrics.snapshot()
            for counter, rate_name in (
                ("frames_captured", "capture_fps"),
                ("frames_entered_encoder", "encoder_input_fps"),
                ("access_units_encoded", "encoded_fps"),
                ("video_access_units_sent", "sent_fps"),
                ("video_bytes_sent", "video_bytes_per_s"),
                ("unitree_frames_received", "unitree_receive_fps"),
                ("unitree_appsrc_frames_pushed", "unitree_appsrc_fps"),
            ):
                value = int(current.get(counter, 0) or 0)
                previous = last_values.get(counter, value)
                self.metrics.set(rate_name, max(0.0, (value - previous) / elapsed))
                last_values[counter] = value
            rate_values = self.metrics.snapshot()
            self.metrics.set(
                "raw_drop_fps",
                max(
                    0.0,
                    float(rate_values.get("capture_fps", 0.0) or 0.0)
                    - float(rate_values.get("encoder_input_fps", 0.0) or 0.0),
                ),
            )
            last_time = now
            print(json.dumps({"vr_video_gateway": self.health_snapshot()}, sort_keys=True), flush=True)
