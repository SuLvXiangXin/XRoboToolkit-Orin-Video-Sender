import threading
import unittest

from vr_gateway.queueing import AccessUnit, AccessUnitQueue, PutResult


class AccessUnitQueueTest(unittest.TestCase):
    def test_queue_starts_at_recovery_boundary(self) -> None:
        queue = AccessUnitQueue(max_items=3, max_bytes=1024, max_age_s=1.0)

        self.assertEqual(queue.put(AccessUnit(b"p", is_recovery_point=False)), PutResult.DROPPED_WAITING_FOR_IDR)
        self.assertEqual(queue.put(AccessUnit(b"idr", is_recovery_point=True)), PutResult.ENQUEUED)
        self.assertEqual(queue.get(timeout_s=0.01).payload, b"idr")

    def test_overflow_flushes_gop_and_drops_until_new_recovery_point(self) -> None:
        queue = AccessUnitQueue(max_items=2, max_bytes=1024, max_age_s=1.0)
        queue.put(AccessUnit(b"idr-1", is_recovery_point=True))
        queue.put(AccessUnit(b"p-1", is_recovery_point=False))

        self.assertEqual(queue.put(AccessUnit(b"p-overflow", is_recovery_point=False)), PutResult.RECOVERY_REQUIRED)
        self.assertTrue(queue.waiting_for_recovery)
        self.assertEqual(queue.put(AccessUnit(b"p-after", is_recovery_point=False)), PutResult.DROPPED_WAITING_FOR_IDR)
        self.assertEqual(queue.put(AccessUnit(b"idr-2", is_recovery_point=True)), PutResult.ENQUEUED)
        self.assertEqual(queue.get(timeout_s=0.01).payload, b"idr-2")
        self.assertIsNone(queue.get(timeout_s=0.01))

    def test_stop_unblocks_waiter(self) -> None:
        queue = AccessUnitQueue(max_items=2, max_bytes=1024, max_age_s=1.0)
        finished = threading.Event()

        thread = threading.Thread(target=lambda: (queue.get(timeout_s=10.0), finished.set()))
        thread.start()
        queue.close()

        self.assertTrue(finished.wait(0.5))
        thread.join(0.5)
        self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
