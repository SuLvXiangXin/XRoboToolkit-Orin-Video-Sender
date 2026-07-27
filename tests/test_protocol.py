import struct
import unittest

from vr_gateway.h264 import ParameterSetCache, annexb_nal_types
from vr_gateway.protocol import (
    CameraRequest,
    ControlFrameDecoder,
    ProtocolError,
    decode_control_command,
    encode_camera_request,
    encode_control_command,
    pack_video_access_unit,
)


class ProtocolTest(unittest.TestCase):
    def test_open_camera_round_trip_matches_wire_endianness(self) -> None:
        request = CameraRequest(
            width=1280,
            height=720,
            fps=30,
            bitrate=4_000_000,
            enable_mv_hevc=0,
            render_mode=2,
            port=12345,
            camera="ZED",
            ip="192.168.50.20",
        )

        frame = encode_control_command("OPEN_CAMERA", encode_camera_request(request))
        self.assertEqual(struct.unpack(">I", frame[:4])[0], len(frame) - 4)

        decoder = ControlFrameDecoder()
        bodies = decoder.feed(frame)
        command = decode_control_command(bodies[0])
        self.assertEqual(command.name, "OPEN_CAMERA")
        self.assertEqual(CameraRequest.decode(command.data), request)

    def test_incremental_decoder_handles_fragmented_and_coalesced_frames(self) -> None:
        first = encode_control_command("OPEN_CAMERA", b"abc")
        second = encode_control_command("CLOSE_CAMERA", b"")
        decoder = ControlFrameDecoder()

        self.assertEqual(decoder.feed(first[:2]), [])
        self.assertEqual(decoder.feed(first[2:7]), [])
        bodies = decoder.feed(first[7:] + second)

        self.assertEqual([decode_control_command(body).name for body in bodies], ["OPEN_CAMERA", "CLOSE_CAMERA"])

    def test_decoder_rejects_oversized_control_frame_before_buffering_body(self) -> None:
        decoder = ControlFrameDecoder(max_body_bytes=32)
        with self.assertRaises(ProtocolError):
            decoder.feed(struct.pack(">I", 33))

    def test_video_frame_is_four_byte_big_endian_length_plus_payload(self) -> None:
        payload = b"\x00\x00\x00\x01\x65payload"
        packet = pack_video_access_unit(payload)
        self.assertEqual(packet[:4], struct.pack(">I", len(payload)))
        self.assertEqual(packet[4:], payload)

    def test_parameter_set_cache_builds_sps_pps_idr_recovery_access_unit(self) -> None:
        sps = b"\x00\x00\x00\x01\x67sps"
        pps = b"\x00\x00\x01\x68pps"
        idr = b"\x00\x00\x00\x01\x65idr"
        cache = ParameterSetCache()
        cache.observe(sps + pps)

        recovery = cache.ensure_recovery_point(idr)

        self.assertEqual(set(annexb_nal_types(recovery)), {5, 7, 8})
        self.assertTrue(cache.is_recovery_point(recovery))


if __name__ == "__main__":
    unittest.main()
