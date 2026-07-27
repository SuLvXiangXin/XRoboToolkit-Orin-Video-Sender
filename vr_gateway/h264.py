from __future__ import annotations


def annexb_nal_units(payload: bytes) -> list[tuple[int, bytes]]:
    starts: list[tuple[int, int]] = []
    index = 0
    while index + 3 <= len(payload):
        if payload[index : index + 4] == b"\x00\x00\x00\x01":
            starts.append((index, 4))
            index += 4
        elif payload[index : index + 3] == b"\x00\x00\x01":
            starts.append((index, 3))
            index += 3
        else:
            index += 1

    result: list[tuple[int, bytes]] = []
    for item_index, (start, prefix_size) in enumerate(starts):
        body_start = start + prefix_size
        body_end = starts[item_index + 1][0] if item_index + 1 < len(starts) else len(payload)
        if body_start >= body_end:
            continue
        nal_type = payload[body_start] & 0x1F
        result.append((nal_type, b"\x00\x00\x00\x01" + payload[body_start:body_end]))
    return result


def annexb_nal_types(payload: bytes) -> tuple[int, ...]:
    return tuple(nal_type for nal_type, _nal in annexb_nal_units(payload))


class ParameterSetCache:
    def __init__(self) -> None:
        self._sps: bytes | None = None
        self._pps: bytes | None = None

    def observe(self, payload: bytes) -> None:
        for nal_type, nal in annexb_nal_units(payload):
            if nal_type == 7:
                self._sps = nal
            elif nal_type == 8:
                self._pps = nal

    def ensure_recovery_point(self, payload: bytes) -> bytes:
        self.observe(payload)
        types = set(annexb_nal_types(payload))
        if 5 not in types:
            return payload
        prefix = bytearray()
        if 7 not in types and self._sps is not None:
            prefix.extend(self._sps)
        if 8 not in types and self._pps is not None:
            prefix.extend(self._pps)
        return bytes(prefix) + payload

    @staticmethod
    def is_recovery_point(payload: bytes) -> bool:
        types = set(annexb_nal_types(payload))
        return {5, 7, 8}.issubset(types)
