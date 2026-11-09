"""Long-running desktop collector for a J2208-family band.

Holds one persistent BLE connection and writes to SQLite or PostgreSQL.
Reconnects on drop. Runs entirely on the PC - the phone and the companion app
are not involved.

Storage is selected with --backend or J2208_DB_BACKEND (sqlite|postgres).
PostgreSQL settings come from J2208_PG_* / standard PG* environment variables;
see tools/j2208_store.py and README.md.

Two collection modes, which differ enormously in battery cost:

  history  Poll the band's own stored records. The band logs HR every 10 min
           and skin temperature every 5 min regardless of who is connected,
           so this adds essentially nothing beyond the radio link. Resolution
           is fixed by the firmware, not by --interval.

  live     Run an on-demand measurement burst each interval. This powers the
           optical front-end (green LEDs + photodiode), which dominates the
           band's power draw. Resolution is per-interval, cost is roughly
           (burst / interval) x continuous-measurement drain.

Battery percentage is recorded on every cycle so actual drain can be measured
rather than guessed - see the `battery` table.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

from bleak import BleakClient, BleakError

sys.path.insert(0, str(Path(__file__).parent))
from j2208 import (  # noqa: E402
    CHAR_NOTIFY,
    CHAR_WRITE,
    HISTORY,
    MODE_CONTINUE,
    MODE_START,
    OP_GET_BATTERY,
    OP_MEASUREMENT_WITH_TYPE,
    SERVICE,
    decode,
    frame,
    parse_records,
    parse_sleep_packet,
    totals_stride,
)
from j2208_store import (  # noqa: E402
    Store,
    backend_from_env,
    make_store,
    normalise_device_id,
)

# Which history types are worth polling on a loop. Excludes the ones that are
# empty on V2 firmware (0x54 hr_dynamic, 0x60 spo2, 0x62 temp).
POLL_TYPES = ["hr", "temp_axil", "spo2_auto", "hrv", "steps", "total", "sleep"]

# Measurement fields that make a live frame worth storing. A frame may carry
# only cached HRV/BP/stress values while the optical sensor warms up, so the
# old hr != 0 test discarded useful rows.
LIVE_FIELDS = ("hr", "spo2", "hrv", "stress", "bp_sys", "bp_dia", "temp_c")

# The band sends a short (~10 frame) measurement burst and then goes quiet.
# In continuous mode, re-send the start command after this many seconds
# without a measurement notification so the stream does not die.
LIVE_REARM_IDLE = 5.0


def has_live_metrics(rec: dict) -> bool:
    """True when a decoded measurement frame contains any usable field."""
    return any(rec.get(name) for name in LIVE_FIELDS)


class BandSession:
    """One connected session. Owns the notify subscription."""

    def __init__(self, client: BleakClient, store: Store,
                 stop: asyncio.Event | None = None):
        self.client = client
        self.store = store
        self.stop = stop
        self.write_char = None
        self.inbox: asyncio.Queue[bytes] = asyncio.Queue()
        self.measuring = False
        self.rearms = 0
        self.interval_hint = 30.0

    async def setup(self):
        svc = self.client.services.get_service(SERVICE)
        if svc is None:
            raise BleakError(f"service {SERVICE} missing")
        self.write_char = svc.get_characteristic(CHAR_WRITE)
        await self.client.start_notify(CHAR_NOTIFY, self._on_notify)

    def _on_notify(self, _ch, data: bytearray):
        self.inbox.put_nowait(bytes(data))

    async def send(self, packet: bytes):
        no_resp = "write-without-response" in self.write_char.properties
        await self.client.write_gatt_char(
            self.write_char, packet, response=not no_resp
        )

    def drain_inbox(self):
        while not self.inbox.empty():
            self.inbox.get_nowait()

    async def collect(self, op: int, idle: float, overall: float) -> list[bytes]:
        """Gather packets for one opcode until the <op> FF terminator."""
        out: list[bytes] = []
        deadline = time.monotonic() + overall
        last = time.monotonic()
        while time.monotonic() < deadline:
            if self.stop is not None and self.stop.is_set():
                return out
            try:
                data = await asyncio.wait_for(self.inbox.get(), timeout=0.3)
            except asyncio.TimeoutError:
                if time.monotonic() - last > idle:
                    await self.send(frame(op, MODE_CONTINUE))
                    last = time.monotonic()
                continue
            last = time.monotonic()
            if not data or data[0] != op:
                continue
            if data[-2:] == bytes([op, 0xFF]):
                if len(data) > 2:
                    out.append(data[:-2])
                return out
            out.append(data)
        return out

    async def read_battery(self) -> int | None:
        self.drain_inbox()
        await self.send(frame(OP_GET_BATTERY))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                data = await asyncio.wait_for(self.inbox.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            if data and data[0] == OP_GET_BATTERY and len(data) >= 2:
                return data[1]
        return None

    async def sync_history(self, htype: str) -> int:
        spec = HISTORY[htype]
        op, stride, fields = spec["op"], spec["stride"], spec.get("fields")
        self.drain_inbox()
        await self.send(frame(op, MODE_START, 0, 0))
        packets = await self.collect(op, idle=1.5, overall=90)
        rows: list[dict] = []
        for pkt in packets:
            if fields == "sleep":
                blk = parse_sleep_packet(pkt)
                if blk is not None:
                    rows.extend(blk)
                    continue
            st = totals_stride(len(pkt)) if fields == "totals" else stride
            rows.extend(parse_records(pkt, st, fields, spec.get("date_only", False)))
        return await self.store.history(htype, rows)

    async def live_burst(self, mtype: int, seconds: float) -> int:
        """Run the optical sensor for `seconds`, store every reading.

        The band answers a start command with a short (~10 frame) burst and
        then falls silent. In continuous mode (seconds <= 0) the measurement
        is re-armed after LIVE_REARM_IDLE seconds without a frame, so the
        stream keeps producing data. Each re-arm costs the same optical
        lock-on time as a fresh start.
        """
        continuous = seconds <= 0
        self.drain_inbox()
        await self.send(frame(OP_MEASUREMENT_WITH_TYPE, mtype, 1))
        self.measuring = True
        window = self.interval_hint if continuous else seconds

        stored = 0
        deadline = time.monotonic() + window
        last_frame = time.monotonic()
        try:
            while time.monotonic() < deadline:
                if self.stop is not None and self.stop.is_set():
                    break
                try:
                    data = await asyncio.wait_for(self.inbox.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    if (
                        continuous
                        and time.monotonic() - last_frame >= LIVE_REARM_IDLE
                    ):
                        await self.send(frame(OP_MEASUREMENT_WITH_TYPE, mtype, 1))
                        self.rearms += 1
                        last_frame = time.monotonic()
                    continue
                if not data or data[0] != OP_MEASUREMENT_WITH_TYPE or len(data) < 10:
                    continue
                last_frame = time.monotonic()
                rec = decode(data)
                if has_live_metrics(rec):
                    stored += await self.store.live(rec)
        finally:
            if not continuous:
                await self.send(frame(OP_MEASUREMENT_WITH_TYPE, mtype, 0))
                self.measuring = False
        return stored


async def run(args) -> int:
    device_id = normalise_device_id(args.device_id or args.address)
    store = await make_store(
        backend=args.backend,
        sqlite_path=args.db,
        device_id=device_id,
    )
    try:
        await store.event("start", json.dumps(vars(args)))
    except Exception:
        await store.close()
        raise

    stop = asyncio.Event()

    def _sig(*_):
        print("\nshutting down...")
        stop.set()

    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, _sig)
        loop.add_signal_handler(signal.SIGTERM, _sig)
    except (NotImplementedError, RuntimeError, ValueError):
        signal.signal(signal.SIGINT, _sig)
        signal.signal(signal.SIGTERM, _sig)

    cycle = 0
    backoff = 5
    print(
        f"collector: mode={args.mode} backend={args.backend} "
        f"interval={args.interval}s device={device_id}"
    )
    print(f"storage: {getattr(store, 'backend', args.backend)}")
    print("Ctrl-C to stop\n")

    try:
        while not stop.is_set():
            try:
                async with BleakClient(args.address, timeout=args.timeout) as client:
                    s = BandSession(client, store, stop)
                    s.interval_hint = max(5.0, args.interval - 3.0)
                    await s.setup()
                    backoff = 5
                    await store.event("connected")
                    print(f"[{datetime.now():%H:%M:%S}] connected")

                    while not stop.is_set():
                        cycle += 1
                        t0 = time.monotonic()
                        line = [f"[{datetime.now():%H:%M:%S}] #{cycle}"]

                        pct = await s.read_battery()
                        if pct is not None:
                            await store.battery(pct, args.mode)
                            line.append(f"batt={pct}%")

                        if args.mode == "live":
                            before_rearms = s.rearms
                            n = await s.live_burst(args.measure_type, args.burst)
                            line.append(f"live+{n}")
                            if s.rearms > before_rearms:
                                line.append(f"rearm+{s.rearms - before_rearms}")

                        # History sync is cheap but slow; do it every Nth cycle.
                        if cycle == 1 or cycle % args.history_every == 0:
                            for htype in args.types:
                                if stop.is_set():
                                    break
                                try:
                                    new = await s.sync_history(htype)
                                    if new:
                                        line.append(f"{htype}+{new}")
                                except Exception as e:
                                    line.append(f"{htype}!{type(e).__name__}")

                        print("  ".join(line))

                        if args.once:
                            stop.set()
                            break

                        spent = time.monotonic() - t0
                        try:
                            await asyncio.wait_for(
                                stop.wait(),
                                timeout=max(1.0, args.interval - spent),
                            )
                        except asyncio.TimeoutError:
                            pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                if stop.is_set():
                    break
                try:
                    await store.event("disconnected", f"{type(e).__name__}: {e}")
                except Exception as db_exc:
                    print(f"could not record disconnect event: {db_exc}")
                print(f"[{datetime.now():%H:%M:%S}] lost connection "
                      f"({type(e).__name__}), retry in {backoff}s")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 120)
    finally:
        try:
            await store.event("stop")
        except Exception as exc:
            print(f"could not record stop event: {exc}")
        try:
            print("\n" + await store.drain_summary())
            counts = await store.stats()
        except Exception as exc:
            print(f"could not read storage summary: {exc}")
            counts = {}
        print(f"live readings: {counts.get('live', 0)}")
        for name, count in sorted(counts.items()):
            if name != "live":
                print(f"  {name:<12} {count}")
        await store.close()
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("address")
    p.add_argument("--mode", choices=["history", "live"], default="history",
                   help="history = near-zero extra drain; live = powers the LEDs")
    p.add_argument("--interval", type=float, default=60.0)
    p.add_argument("--burst", type=float, default=25.0,
                   help="live mode: seconds of measurement per interval. The "
                        "sensor needs ~9s to lock on, so <15s yields nothing. "
                        "Use 0 to leave it running continuously.")
    p.add_argument("--measure-type", type=int, default=2,
                   help="2=heart (fastest), 3=spo2, 4=temp, 1=hrv")
    p.add_argument("--history-every", type=int, default=10,
                   help="run a history sync every Nth cycle")
    p.add_argument("--types", nargs="+", default=POLL_TYPES, choices=POLL_TYPES,
                   help="which history types to sync. A full sweep takes ~90s; "
                        "use '--types hr' if heart rate is all you need.")
    p.add_argument("--backend", choices=["sqlite", "postgres"],
                   default=backend_from_env(),
                   help="storage backend (default: J2208_DB_BACKEND or sqlite)")
    p.add_argument("--device-id", default=os.environ.get("J2208_DEVICE_ID"),
                   help="device identifier (default: BLE address)")
    p.add_argument("--once", action="store_true",
                   help="run one collection cycle and exit")
    p.add_argument("--db", default="captures/j2208.db",
                   help="SQLite database path (ignored for postgres)")
    p.add_argument("--timeout", type=float, default=20.0)
    args = p.parse_args()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except ValueError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
