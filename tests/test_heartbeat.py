import json
from pathlib import Path
import unittest

from vr_gateway.heartbeat import HeartbeatError, ReceiverHealth, decode_heartbeat


UNITY_HEARTBEAT_FIXTURE = (
    Path(__file__).parent / "fixtures" / "unity_receiver_heartbeat_v1.json"
)


def heartbeat(
    sequence: int,
    displayed_frames: int,
    state: str = "playing",
    visual_profile_available: bool = False,
) -> bytes:
    return json.dumps(
        {
            "version": 1,
            "session_id": "pico-session",
            "sequence": sequence,
            "displayed_frames": displayed_frames,
            "decoded_frames": displayed_frames + 1,
            "unity_update_hz": 72.0,
            "decoder_state": state,
            "visual_profile_available": visual_profile_available,
        }
    ).encode()


class HeartbeatTest(unittest.TestCase):
    def test_decodes_actual_unity_canonical_payload(self) -> None:
        decoded = decode_heartbeat(UNITY_HEARTBEAT_FIXTURE.read_bytes().strip())

        self.assertEqual(decoded.session_id, "session-a")
        self.assertEqual(decoded.sequence, 42)
        self.assertEqual(decoded.displayed_frames, 99)
        self.assertEqual(decoded.decoded_frames, 100)
        self.assertAlmostEqual(decoded.unity_update_hz, 89.9)
        self.assertEqual(decoded.decoder_state, "playing")
        self.assertTrue(decoded.visual_profile_available)

    def test_decode_rejects_unknown_version(self) -> None:
        with self.assertRaises(HeartbeatError):
            decode_heartbeat(b'{"version":2}')

    def test_decode_accepts_schema_states_and_rejects_unknown_or_zero_sequence(self) -> None:
        for state in ("starting", "playing", "stalled", "disconnected", "error", "stopped"):
            with self.subTest(state=state):
                self.assertEqual(decode_heartbeat(heartbeat(1, 0, state)).decoder_state, state)
        with self.assertRaises(HeartbeatError):
            decode_heartbeat(heartbeat(1, 0, "ready"))
        with self.assertRaises(HeartbeatError):
            decode_heartbeat(heartbeat(0, 0))

        malformed = json.loads(heartbeat(1, 0))
        malformed["visual_profile_available"] = "false"
        with self.assertRaises(HeartbeatError):
            decode_heartbeat(json.dumps(malformed).encode())

    def test_observation_only_heartbeat_is_healthy_but_not_visual_authority(self) -> None:
        health = ReceiverHealth(heartbeat_stale_s=2.0, display_stale_s=1.0)
        decoded = decode_heartbeat(heartbeat(1, 1, visual_profile_available=False))
        health.update(decoded, ("127.0.0.1", 1), now=1.0)

        snapshot = health.snapshot(now=1.1)

        self.assertTrue(snapshot["receiver_healthy"])
        self.assertFalse(snapshot["receiver_visual_profile_available"])

    def test_display_progress_age_only_refreshes_when_displayed_count_advances(self) -> None:
        health = ReceiverHealth(heartbeat_stale_s=2.0, display_stale_s=1.0)
        health.update(decode_heartbeat(heartbeat(1, 10)), ("192.168.1.20", 1234), now=10.0)
        health.update(decode_heartbeat(heartbeat(2, 10)), ("192.168.1.20", 1234), now=10.5)

        snapshot = health.snapshot(now=11.2)

        self.assertAlmostEqual(snapshot["heartbeat_age_s"], 0.7)
        self.assertAlmostEqual(snapshot["display_progress_age_s"], 1.2)
        self.assertFalse(snapshot["receiver_healthy"])

    def test_new_session_resets_progress_baseline(self) -> None:
        health = ReceiverHealth(heartbeat_stale_s=2.0, display_stale_s=1.0)
        health.update(decode_heartbeat(heartbeat(10, 100)), ("127.0.0.1", 1), now=1.0)
        new_session = json.loads(heartbeat(1, 1))
        new_session["session_id"] = "new"
        health.update(decode_heartbeat(json.dumps(new_session).encode()), ("127.0.0.1", 1), now=2.0)
        self.assertTrue(health.snapshot(now=2.1)["receiver_healthy"])


if __name__ == "__main__":
    unittest.main()
