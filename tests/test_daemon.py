"""Unit tests for daemon helpers (no BLE hardware required)."""

from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from typing import ClassVar
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

try:
    import j2208_daemon
    from j2208_daemon import (
        OP_GET_BATTERY,
        OP_MEASUREMENT_WITH_TYPE,
        BandSession,
        frame,
        has_live_metrics,
    )
except ImportError:  # pragma: no cover - bleak is optional for store-only tests
    j2208_daemon = None
    BandSession = None
    has_live_metrics = None


@unittest.skipUnless(has_live_metrics is not None, "bleak not installed")
class LiveMetricsTests(unittest.TestCase):
    def test_all_zero_is_ignored(self):
        rec = {
            "hr": 0,
            "spo2": 0,
            "hrv": 0,
            "stress": 0,
            "bp_sys": 0,
            "bp_dia": 0,
            "temp_c": 0.0,
        }
        self.assertFalse(has_live_metrics(rec))

    def test_empty_record_is_ignored(self):
        self.assertFalse(has_live_metrics({}))

    def test_hr_is_kept(self):
        self.assertTrue(has_live_metrics({"hr": 63}))

    def test_spo2_only_is_kept(self):
        self.assertTrue(has_live_metrics({"spo2": 99}))

    def test_bp_only_is_kept(self):
        self.assertTrue(has_live_metrics({"bp_sys": 119, "bp_dia": 69}))

    def test_hrv_and_stress_are_kept(self):
        self.assertTrue(has_live_metrics({"hrv": 46, "stress": 11}))

    def test_temp_only_is_kept(self):
        self.assertTrue(has_live_metrics({"temp_c": 36.3}))


class _FakeChar:
    properties: ClassVar[list[str]] = ["write", "write-without-response"]


class _FakeClient:
    def __init__(self):
        self.writes: list[bytes] = []

    async def write_gatt_char(self, _char, data, response=False):
        self.writes.append(bytes(data))


class _FakeStore:
    def __init__(self):
        self.rows: list[dict] = []

    async def live(self, rec: dict) -> bool:
        self.rows.append(rec)
        return True


def _measurement_frame(hr: int = 80, spo2: int = 98) -> bytes:
    return frame(OP_MEASUREMENT_WITH_TYPE, 3, hr, spo2)


def _start_writes(client: _FakeClient) -> list[bytes]:
    return [
        pkt
        for pkt in client.writes
        if pkt[0] == OP_MEASUREMENT_WITH_TYPE and pkt[2] == 1
    ]


def _stop_writes(client: _FakeClient) -> list[bytes]:
    return [
        pkt
        for pkt in client.writes
        if pkt[0] == OP_MEASUREMENT_WITH_TYPE and pkt[2] == 0
    ]


@unittest.skipUnless(BandSession is not None, "bleak not installed")
class BandSessionTests(unittest.IsolatedAsyncioTestCase):
    def _session(self):
        client = _FakeClient()
        store = _FakeStore()
        session = BandSession(client, store)
        session.write_char = _FakeChar()
        session.reader_task = asyncio.create_task(session._read_loop())
        self.addAsyncCleanup(session.close)
        return session, client, store

    async def test_battery_reply_does_not_discard_live_frames(self):
        session, _client, _store = self._session()
        session.live_enabled = True
        session._on_notify(None, bytearray(_measurement_frame()))

        async def reply():
            await asyncio.sleep(0)
            session._on_notify(None, bytearray(bytes([OP_GET_BATTERY, 55])))

        asyncio.create_task(reply())
        pct = await asyncio.wait_for(session.read_battery(), 1)

        self.assertEqual(pct, 55)
        self.assertEqual(session.live_queue.qsize(), 1)

    async def test_continuous_stream_starts_once_and_stores_frames(self):
        session, client, store = self._session()

        await session.start_live(3)
        session._on_notify(None, bytearray(_measurement_frame()))
        for _ in range(10):
            if store.rows:
                break
            await asyncio.sleep(0.01)

        self.assertTrue(store.rows)
        self.assertEqual(len(_start_writes(client)), 1)

        await session.start_live(3)  # already running: must not restart
        self.assertEqual(len(_start_writes(client)), 1)

        await session.close()
        self.assertEqual(len(_stop_writes(client)), 1)

    async def test_history_collection_does_not_stop_live_storage(self):
        session, _client, store = self._session()
        await session.start_live(3)

        live_frame = _measurement_frame()
        history_packet = bytes([0x55, 0, 1, 2, 3, 4, 5, 6, 7, 0x55, 0xFF])
        session._on_notify(None, bytearray(live_frame))
        session._on_notify(None, bytearray(history_packet))

        packets = await session.collect(0x55, idle=100, overall=1)

        self.assertEqual(packets, [history_packet[:-2]])
        for _ in range(10):
            if store.rows:
                break
            await asyncio.sleep(0.01)
        self.assertTrue(store.rows)

    async def test_proactive_keepalive_while_frames_flow(self):
        with (
            mock.patch.object(j2208_daemon, "LIVE_KEEPALIVE", 0.1),
            mock.patch.object(j2208_daemon, "LIVE_REARM_IDLE", 5.0),
        ):
            session, client, _store = self._session()
            await session.start_live(3)

            for _ in range(16):
                session._on_notify(None, bytearray(_measurement_frame()))
                await asyncio.sleep(0.03)

            self.assertEqual(session.rearms, 0)
            self.assertGreaterEqual(session.keepalives, 1)
            self.assertGreaterEqual(len(_start_writes(client)), 2)

    async def test_rearm_is_reactionary_only(self):
        with mock.patch.object(j2208_daemon, "LIVE_REARM_IDLE", 0.1):
            session, client, _store = self._session()
            await session.start_live(3)

            for _ in range(12):
                session._on_notify(None, bytearray(_measurement_frame()))
                await asyncio.sleep(0.03)

            self.assertEqual(len(_start_writes(client)), 1)
            self.assertEqual(session.keepalives, 0)
            self.assertEqual(session.rearms, 0)

            for _ in range(30):
                if session.rearms:
                    break
                await asyncio.sleep(0.05)

            self.assertEqual(session.rearms, 1)
            self.assertEqual(len(_start_writes(client)), 2)


if __name__ == "__main__":
    unittest.main()
