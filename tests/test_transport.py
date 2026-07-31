import socket
import threading
import time
import unittest

from vr_gateway.protocol import unpack_video_access_unit
from vr_gateway.queueing import AccessUnit, AccessUnitQueue
from vr_gateway.transport import Destination, VideoTransport


class FakeSocket:
    def __init__(self, fail_send: bool = False) -> None:
        self.fail_send = fail_send
        self.sent: list[bytes] = []
        self.closed = False

    def setsockopt(self, *_args) -> None:
        return None

    def settimeout(self, _timeout: float) -> None:
        return None

    def sendall(self, packet: bytes) -> None:
        if self.fail_send:
            self.fail_send = False
            raise socket.timeout("injected send timeout")
        self.sent.append(packet)

    def shutdown(self, _how: int) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class VideoTransportTest(unittest.TestCase):
    def test_send_timeout_reconnects_and_resumes_only_from_recovery_access_unit(self) -> None:
        first = FakeSocket(fail_send=True)
        second = FakeSocket()
        sockets = [first, second]
        connected = threading.Condition()
        connect_count = 0

        def connector(_destination: Destination, _timeout_s: float) -> FakeSocket:
            nonlocal connect_count
            with connected:
                result = sockets[connect_count]
                connect_count += 1
                connected.notify_all()
                return result

        recoveries = 0

        def request_recovery() -> None:
            nonlocal recoveries
            recoveries += 1

        queue = AccessUnitQueue(max_items=4, max_bytes=4096, max_age_s=1.0)
        transport = VideoTransport(
            queue,
            connector=connector,
            request_recovery=request_recovery,
            reconnect_delay_s=0.01,
            connect_timeout_s=0.1,
            send_timeout_s=0.1,
        )
        transport.start()
        transport.set_target(Destination("127.0.0.1", 12345))
        time.sleep(0.03)
        self.assertEqual(connect_count, 0)

        queue.put(AccessUnit(b"recovery-1", is_recovery_point=True))
        self.assertTrue(_wait_for(lambda: connect_count >= 1))
        self.assertTrue(_wait_for(lambda: first.closed))

        queue.put(AccessUnit(b"stale-p", is_recovery_point=False))
        queue.put(AccessUnit(b"recovery-2", is_recovery_point=True))
        self.assertTrue(_wait_for(lambda: connect_count >= 2))
        self.assertTrue(_wait_for(lambda: len(second.sent) == 1))
        transport.stop()

        self.assertEqual(unpack_video_access_unit(second.sent[0]), b"recovery-2")
        self.assertGreaterEqual(recoveries, 2)
        self.assertFalse(transport.is_running)


def _wait_for(predicate, timeout_s: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


if __name__ == "__main__":
    unittest.main()
