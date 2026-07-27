from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
import threading
import time
from typing import Callable, Iterable

from .config import EncoderKind, SourceConfig, SourceKind, StreamConfig
from .h264 import ParameterSetCache, annexb_nal_types
from .metrics import Metrics
from .queueing import AccessUnit, AccessUnitQueue, PutResult
from .unitree_source import LatestRawFrameQueue


_ENCODER_FACTORIES = {
    EncoderKind.JETSON: ("nvv4l2h264enc",),
    EncoderKind.NVENC: ("nvh264enc",),
    EncoderKind.VAAPI: ("vah264enc", "vaapih264enc"),
    EncoderKind.X264: ("x264enc",),
}


def encoder_candidates(requested: EncoderKind, available_features: Iterable[str]) -> list[EncoderKind]:
    available = set(available_features)
    ordered = [EncoderKind.JETSON, EncoderKind.NVENC, EncoderKind.VAAPI, EncoderKind.X264]
    candidates = ordered if requested == EncoderKind.AUTO else [requested]
    return [
        candidate
        for candidate in candidates
        if any(factory in available for factory in _ENCODER_FACTORIES[candidate])
    ]


def resolve_encoder_factory(kind: EncoderKind, available_features: Iterable[str]) -> str:
    available = set(available_features)
    for factory in _ENCODER_FACTORIES[kind]:
        if factory in available:
            return factory
    raise RuntimeError(f"GStreamer encoder is unavailable: {kind.value}")


def build_pipeline_description(
    stream: StreamConfig,
    source: SourceConfig,
    encoder: EncoderKind,
    *,
    encoder_factory: str | None = None,
) -> str:
    stream.validate()
    source.validate()
    factory = encoder_factory or _ENCODER_FACTORIES[encoder][0]
    source_description = _source_description(source, stream)
    encoder_description = _encoder_description(encoder, factory)
    raw_format = "I420" if encoder == EncoderKind.X264 else "NV12"
    raw_caps = (
        f"video/x-raw,format={raw_format},width={stream.width},height={stream.height},"
        f"framerate={stream.fps}/1"
    )
    return (
        f"{source_description} ! identity name=capture_probe signal-handoffs=true ! "
        "queue name=raw_latest max-size-buffers=1 max-size-bytes=0 "
        "max-size-time=0 leaky=downstream ! "
        "identity name=encoder_input_probe signal-handoffs=true ! "
        f"videoconvert ! videoscale ! videorate ! {raw_caps} ! "
        f"{encoder_description} ! "
        "h264parse name=stream_parser config-interval=-1 ! "
        "video/x-h264,stream-format=byte-stream,alignment=au ! "
        "appsink name=encoded_sink emit-signals=true sync=false "
        "max-buffers=2 drop=false"
    )


def available_gstreamer_features() -> set[str]:
    Gst, _GstVideo = _load_gstreamer()
    registry = Gst.Registry.get()
    return {
        factory
        for factories in _ENCODER_FACTORIES.values()
        for factory in factories
        if registry.find_feature(factory, Gst.ElementFactory) is not None
    }


class GstCapturePipeline:
    """GStreamer capture/encode plane whose callbacks never perform network I/O."""

    def __init__(
        self,
        stream: StreamConfig,
        source: SourceConfig,
        encoder: EncoderKind,
        queue: AccessUnitQueue,
        metrics: Metrics,
        *,
        error_callback: Callable[[str], None] | None = None,
        raw_frame_source: LatestRawFrameQueue | None = None,
    ) -> None:
        self.stream = stream
        self.source = source
        self.encoder = encoder
        self.queue = queue
        self.metrics = metrics
        self.error_callback = error_callback
        self.raw_frame_source = raw_frame_source
        self._pipeline = None
        self._encoder_element = None
        self._sink = None
        self._probe = None
        self._encoder_input_probe = None
        self._appsrc = None
        self._stop = threading.Event()
        self._keyframe_request = threading.Event()
        self._bus_thread: threading.Thread | None = None
        self._appsrc_thread: threading.Thread | None = None
        self._parameter_sets = ParameterSetCache()
        self._capture_times: OrderedDict[int, int] = OrderedDict()
        self._capture_times_lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        return self._pipeline is not None

    def start(self) -> None:
        if self._pipeline is not None:
            return
        Gst, _GstVideo = _load_gstreamer()
        features = available_gstreamer_features()
        factory = resolve_encoder_factory(self.encoder, features)
        description = build_pipeline_description(
            self.stream,
            self.source,
            self.encoder,
            encoder_factory=factory,
        )
        self.metrics.set("encoder", self.encoder.value)
        self.metrics.set("encoder_factory", factory)
        self.metrics.set("pipeline", description)
        try:
            pipeline = Gst.parse_launch(description)
        except Exception as exc:
            raise RuntimeError(f"failed to create GStreamer pipeline: {exc}") from exc
        encoder_element = pipeline.get_by_name("video_encoder")
        sink = pipeline.get_by_name("encoded_sink")
        probe = pipeline.get_by_name("capture_probe")
        encoder_input_probe = pipeline.get_by_name("encoder_input_probe")
        appsrc = pipeline.get_by_name("unitree_appsrc")
        if encoder_element is None or sink is None or probe is None or encoder_input_probe is None:
            pipeline.set_state(Gst.State.NULL)
            raise RuntimeError("GStreamer pipeline is missing named gateway elements")
        if self.source.kind == SourceKind.UNITREE_WEBRTC and (
            appsrc is None or self.raw_frame_source is None
        ):
            pipeline.set_state(Gst.State.NULL)
            raise RuntimeError("unitree-webrtc pipeline requires its raw appsrc frame source")

        self._configure_encoder(encoder_element)
        sink.connect("new-sample", self._on_new_sample)
        probe.connect("handoff", self._on_capture_handoff)
        encoder_input_probe.connect("handoff", self._on_encoder_input_handoff)
        self._pipeline = pipeline
        self._encoder_element = encoder_element
        self._sink = sink
        self._probe = probe
        self._encoder_input_probe = encoder_input_probe
        self._appsrc = appsrc
        self._stop.clear()
        if appsrc is not None:
            self._appsrc_thread = threading.Thread(
                target=self._appsrc_loop,
                name="unitree-appsrc",
                daemon=True,
            )
            self._appsrc_thread.start()
        state_result = pipeline.set_state(Gst.State.PLAYING)
        if state_result == Gst.StateChangeReturn.FAILURE:
            self.stop()
            raise RuntimeError(f"failed to start {self.encoder.value} GStreamer pipeline")
        settled, state, _pending = pipeline.get_state(5 * Gst.SECOND)
        if settled == Gst.StateChangeReturn.FAILURE or state == Gst.State.NULL:
            self.stop()
            raise RuntimeError(f"{self.encoder.value} pipeline did not reach a running state")
        self._bus_thread = threading.Thread(target=self._bus_loop, name="gstreamer-bus", daemon=True)
        self._bus_thread.start()
        self.metrics.set("pipeline_running", True)

    def stop(self) -> None:
        Gst, _GstVideo = _load_gstreamer()
        self._stop.set()
        pipeline, self._pipeline = self._pipeline, None
        appsrc, self._appsrc = self._appsrc, None
        if appsrc is not None:
            try:
                appsrc.emit("end-of-stream")
            except Exception:
                pass
        if pipeline is not None:
            pipeline.set_state(Gst.State.NULL)
        if self._appsrc_thread is not None and self._appsrc_thread is not threading.current_thread():
            self._appsrc_thread.join(timeout=1.0)
        self._appsrc_thread = None
        if self._bus_thread is not None and self._bus_thread is not threading.current_thread():
            self._bus_thread.join(timeout=1.0)
        self._bus_thread = None
        self._encoder_element = None
        self._sink = None
        self._probe = None
        self._encoder_input_probe = None
        self.metrics.set("pipeline_running", False)

    def request_keyframe(self) -> None:
        self._keyframe_request.set()

    def _on_capture_handoff(self, _identity, buffer) -> None:
        Gst, _GstVideo = _load_gstreamer()
        captured_ns = time.monotonic_ns()
        self.metrics.inc("frames_captured")
        self.metrics.set("last_capture_monotonic_ns", captured_ns)
        if buffer.pts != Gst.CLOCK_TIME_NONE:
            with self._capture_times_lock:
                self._capture_times[int(buffer.pts)] = captured_ns
                while len(self._capture_times) > 512:
                    self._capture_times.popitem(last=False)

    def _on_encoder_input_handoff(self, _identity, _buffer) -> None:
        self.metrics.inc("frames_entered_encoder")

    def _appsrc_loop(self) -> None:
        Gst, _GstVideo = _load_gstreamer()
        source = self.raw_frame_source
        appsrc = self._appsrc
        assert source is not None and appsrc is not None
        expected_size = self.stream.width * self.stream.height * 3
        max_age_ns = int(self.source.unitree_frame_timeout_s * 1e9)
        while not self._stop.is_set():
            frame = source.get(timeout_s=0.05)
            if frame is None:
                continue
            if frame.width != self.stream.width or frame.height != self.stream.height:
                self.metrics.inc("unitree_appsrc_shape_errors")
                continue
            if len(frame.data) != expected_size:
                self.metrics.inc("unitree_appsrc_size_errors")
                continue
            now_ns = time.monotonic_ns()
            if now_ns - frame.captured_monotonic_ns > max_age_ns:
                self.metrics.inc("unitree_appsrc_stale_drops")
                continue
            buffer = Gst.Buffer.new_allocate(None, len(frame.data), None)
            buffer.fill(0, frame.data)
            buffer.pts = Gst.CLOCK_TIME_NONE
            buffer.dts = Gst.CLOCK_TIME_NONE
            buffer.duration = Gst.SECOND // self.stream.fps
            result = appsrc.emit("push-buffer", buffer)
            if result != Gst.FlowReturn.OK:
                if not self._stop.is_set():
                    self.metrics.inc("unitree_appsrc_push_errors")
                continue
            self.metrics.inc("unitree_appsrc_frames_pushed")
            self.metrics.set("last_unitree_appsrc_monotonic_ns", now_ns)
            self.metrics.set(
                "unitree_source_to_appsrc_ms",
                max(0.0, (now_ns - frame.captured_monotonic_ns) * 1e-6),
            )

    def _on_new_sample(self, sink):
        Gst, _GstVideo = _load_gstreamer()
        sample = sink.emit("pull-sample")
        if sample is None:
            return Gst.FlowReturn.ERROR
        buffer = sample.get_buffer()
        if buffer is None:
            return Gst.FlowReturn.ERROR
        payload = buffer.extract_dup(0, buffer.get_size())
        self._parameter_sets.observe(payload)
        nal_types = set(annexb_nal_types(payload))
        is_keyframe = 5 in nal_types or not buffer.has_flags(Gst.BufferFlags.DELTA_UNIT)
        if is_keyframe:
            payload = self._parameter_sets.ensure_recovery_point(payload)
        recovery = self._parameter_sets.is_recovery_point(payload)
        pts_ns = None if buffer.pts == Gst.CLOCK_TIME_NONE else int(buffer.pts)
        captured_ns = time.monotonic_ns()
        if pts_ns is not None:
            with self._capture_times_lock:
                captured_ns = self._capture_times.pop(pts_ns, captured_ns)
        self.metrics.set("capture_to_encode_ms", max(0.0, (time.monotonic_ns() - captured_ns) * 1e-6))
        result = self.queue.put(
            AccessUnit(
                payload=payload,
                is_recovery_point=recovery,
                pts_ns=pts_ns,
                captured_monotonic_ns=captured_ns,
            )
        )
        self.metrics.inc("access_units_encoded")
        self.metrics.inc("encoded_bytes", len(payload))
        if recovery:
            self.metrics.inc("recovery_access_units")
        if result != PutResult.ENQUEUED:
            self.metrics.inc(f"queue_{result.value}")
        if result == PutResult.RECOVERY_REQUIRED:
            self._keyframe_request.set()
        return Gst.FlowReturn.OK

    def _bus_loop(self) -> None:
        Gst, GstVideo = _load_gstreamer()
        assert self._pipeline is not None
        bus = self._pipeline.get_bus()
        while not self._stop.is_set():
            if self._keyframe_request.is_set():
                self._keyframe_request.clear()
                if self._send_force_key_unit(Gst, GstVideo):
                    self.metrics.inc("keyframe_requests")
                else:
                    self.metrics.inc("keyframe_request_errors")
            message = bus.timed_pop_filtered(
                50 * Gst.MSECOND,
                Gst.MessageType.ERROR | Gst.MessageType.EOS,
            )
            if message is None:
                continue
            if message.type == Gst.MessageType.EOS and self.source.kind == SourceKind.FILE:
                if not self._pipeline.seek_simple(
                    Gst.Format.TIME,
                    Gst.SeekFlags.FLUSH | Gst.SeekFlags.KEY_UNIT,
                    0,
                ):
                    self._report_error("failed to loop video file")
                continue
            if message.type == Gst.MessageType.ERROR:
                error, debug = message.parse_error()
                self._report_error(f"GStreamer error: {error}; {debug or ''}")
            else:
                self._report_error("GStreamer source reached EOS")
            return

    def _send_force_key_unit(self, Gst, GstVideo) -> bool:
        encoder = self._encoder_element
        if encoder is None:
            return False
        source_pad = encoder.get_static_pad("src")
        if source_pad is None:
            return False
        event = GstVideo.video_event_new_upstream_force_key_unit(
            Gst.CLOCK_TIME_NONE,
            True,
            0,
        )
        return bool(source_pad.send_event(event))

    def _configure_encoder(self, encoder) -> None:
        bitrate_kbps = max(1, self.stream.bitrate // 1000)
        if self.encoder == EncoderKind.JETSON:
            _set_property(encoder, "bitrate", self.stream.bitrate)
            _set_property(encoder, "idrinterval", self.stream.gop_frames)
            _set_property(encoder, "iframeinterval", self.stream.gop_frames)
            _set_property(encoder, "insert-sps-pps", True)
            _set_property(encoder, "maxperf-enable", True)
            _set_property(encoder, "num-B-Frames", 0)
        elif self.encoder == EncoderKind.NVENC:
            _set_property(encoder, "bitrate", bitrate_kbps)
            _set_property(encoder, "gop-size", self.stream.gop_frames)
            _set_property(encoder, "bframes", 0)
            _set_property(encoder, "rc-lookahead", 0)
            _set_property(encoder, "zerolatency", True)
            _set_property_from_string(encoder, "preset", "low-latency-hp")
            _set_property_from_string(encoder, "rc-mode", "cbr")
        elif self.encoder == EncoderKind.VAAPI:
            _set_property(encoder, "bitrate", bitrate_kbps)
            _set_property(encoder, "keyframe-period", self.stream.gop_frames)
            _set_property(encoder, "b-frames", 0)
            _set_property(encoder, "max-bframes", 0)
            _set_property_from_string(encoder, "rate-control", "cbr")
        else:
            _set_property(encoder, "bitrate", bitrate_kbps)
            _set_property(encoder, "key-int-max", self.stream.gop_frames)
            _set_property(encoder, "bframes", 0)
            _set_property(encoder, "rc-lookahead", 0)
            _set_property(encoder, "sync-lookahead", 0)
            _set_property_from_string(encoder, "speed-preset", "ultrafast")
            _set_property_from_string(encoder, "tune", "zerolatency")

    def _report_error(self, message: str) -> None:
        self.metrics.inc("pipeline_errors")
        self.metrics.set("pipeline_last_error", message)
        if self.error_callback is not None:
            self.error_callback(message)


def _source_description(source: SourceConfig, stream: StreamConfig) -> str:
    if source.kind == SourceKind.SYNTHETIC:
        return "videotestsrc is-live=true do-timestamp=true pattern=smpte ! timeoverlay"
    if source.kind == SourceKind.FILE:
        uri = Path(source.path).resolve().as_uri().replace('"', '%22')
        return (
            f'uridecodebin uri="{uri}" caps="video/x-raw(ANY)" '
            "expose-all-streams=false"
        )
    if source.kind == SourceKind.UNITREE_WEBRTC:
        return (
            "appsrc name=unitree_appsrc is-live=true do-timestamp=true "
            "format=time block=false emit-signals=false max-buffers=1 "
            "max-bytes=0 max-time=0 leaky-type=downstream "
            f'caps="video/x-raw,format=RGB,width={stream.width},height={stream.height},'
            f'framerate={stream.fps}/1"'
        )
    device = source.path or "/dev/video0"
    escaped = device.replace("\\", "\\\\").replace('"', '\\"')
    if source.uvc_mode == "mjpeg":
        return (
            f'v4l2src device="{escaped}" do-timestamp=true ! '
            f'image/jpeg,width={stream.width},height={stream.height},framerate={stream.fps}/1 ! jpegdec'
        )
    return (
        f'v4l2src device="{escaped}" do-timestamp=true ! '
        f'video/x-raw,width={stream.width},height={stream.height},framerate={stream.fps}/1'
    )


def _encoder_description(encoder: EncoderKind, factory: str) -> str:
    if encoder == EncoderKind.JETSON:
        return f"nvvidconv ! video/x-raw(memory:NVMM),format=NV12 ! {factory} name=video_encoder"
    return f"{factory} name=video_encoder"


def _set_property(element, name: str, value) -> None:
    if element.find_property(name) is not None:
        element.set_property(name, value)


def _set_property_from_string(element, name: str, value: str) -> None:
    if element.find_property(name) is None:
        return
    Gst, _GstVideo = _load_gstreamer()
    Gst.util_set_object_arg(element, name, value)


_GST_MODULES = None


def _load_gstreamer():
    global _GST_MODULES
    if _GST_MODULES is None:
        try:
            import gi

            gi.require_version("Gst", "1.0")
            gi.require_version("GstVideo", "1.0")
            from gi.repository import Gst, GstVideo
        except (ImportError, ValueError) as exc:
            raise RuntimeError(
                "Python GObject GStreamer bindings are required (python3-gi and gir1.2-gstreamer-1.0)"
            ) from exc
        Gst.init(None)
        _GST_MODULES = (Gst, GstVideo)
    return _GST_MODULES
