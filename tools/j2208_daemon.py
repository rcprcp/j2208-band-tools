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

  live     Power the optical sensor. With --burst 0, one measurement stream is
           held open for the life of the BLE connection and kept alive across
           collection cycles. While frames are still arriving, a fresh start is
           sent every 20 s to roll the firmware's ~28 s measurement session over
           before it expires, keeping HR/SpO2 continuous without a new optical
           lock-on. If the stream goes silent, a reactionary re-arm follows.
           With a positive --burst, a measurement runs for that many seconds
           each interval. Resolution is per-notification, cost is roughly
           continuous-measurement drain.

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
from contextlib import suppress
from datetime import datetime
from pathlib import Path

from bleak import BleakClient, BleakError

sys.path.insert(0, str(Path(__file__).parent))
from j2208 import (
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
from j2208_store import (
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

# Proactive keep-alive for continuous mode. The firmware ends a type-3
# measurement session after ~28 s; re-sending the start while frames are still
# arriving rolls the session over without dropping the optical lock. This was
# verified on the band: 20 s keep-alives produced continuous HR and, after the
# initial Sp O2 lock, continuous SpO2 for 70-80 s. It is not a hard guarantee,
# so the reactionary re-arm below remains the fallback.
LIVE_KEEPALIVE = 20.0

# Reactionary re-arm: if the stream does go quiet despite the keep-alive,
# re-send the start command after this many seconds without a measurement
# notification. Each reactionary re-arm pays the optical lock-on cost again.
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
        self.live_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self.history_queues: dict[int, asyncio.Queue[bytes]] = {}
        self.battery_future: asyncio.Future[bytes] | None = None
        self.send_lock = asyncio.Lock()
        self.reader_task: asyncio.Task | None = None
        self.live_task: asyncio.Task | None = None
        self.live_enabled = False
        self.live_measure_type = 2
        self.live_stored = 0
        self.live_error: BaseException | None = None
        self.measuring = False
        self.keepalives = 0
        self.rearms = 0

    async def setup(self):
        svc = self.client.services.get_service(SERVICE)
        if svc is None:
            raise BleakError(f"service {SERVICE} missing")
        self.write_char = svc.get_characteristic(CHAR_WRITE)
        await self.client.start_notify(CHAR_NOTIFY, self._on_notify)
        self.reader_task = asyncio.create_task(
            self._read_loop(), name="j2208-reader"
        )

    def _on_notify(self, _ch, data: bytearray):
        self.inbox.put_nowait(bytes(data))

    async def _read_loop(self):
        """Dispatch notifications by opcode so no consumer can drop another's."""
        while True:
            data = await self.inbox.get()
            if not data:
                continue
            op = data[0]
            if op == OP_MEASUREMENT_WITH_TYPE:
                if self.live_enabled:
                    self.live_queue.put_nowait(data)
                continue
            if op == OP_GET_BATTERY:
                fut = self.battery_future
                if fut is not None and not fut.done():
                    fut.set_result(data)
                continue
            self.history_queues.setdefault(op, asyncio.Queue()).put_nowait(data)

    def _history_queue(self, op: int) -> asyncio.Queue[bytes]:
        return self.history_queues.setdefault(op, asyncio.Queue())

    def _drain_live_queue(self):
        while not self.live_queue.empty():
            self.live_queue.get_nowait()

    async def send(self, packet: bytes):
        no_resp = "write-without-response" in self.write_char.properties
        async with self.send_lock:
            await self.client.write_gatt_char(
                self.write_char, packet, response=not no_resp
            )

    async def collect(self, op: int, idle: float, overall: float) -> list[bytes]:
        """Gather packets for one opcode until the <op> FF terminator."""
        queue = self._history_queue(op)
        out: list[bytes] = []
        deadline = time.monotonic() + overall
        last = time.monotonic()
        while time.monotonic() < deadline:
            if self.stop is not None and self.stop.is_set():
                return out
            try:
                data = await asyncio.wait_for(queue.get(), timeout=0.3)
            except TimeoutError:
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
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.battery_future = fut
        try:
            await self.send(frame(OP_GET_BATTERY))
            data = await asyncio.wait_for(fut, timeout=5)
        except TimeoutError:
            return None
        finally:
            if self.battery_future is fut:
                self.battery_future = None
        if data and data[0] == OP_GET_BATTERY and len(data) >= 2:
            return data[1]
        return None

    async def sync_history(self, htype: str) -> int:
        spec = HISTORY[htype]
        op, stride, fields = spec["op"], spec["stride"], spec.get("fields")
        queue = self._history_queue(op)
        while not queue.empty():
            queue.get_nowait()
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

    async def start_live(self, mtype: int) -> None:
        """Start the one long-lived measurement stream for this connection.

        While frames keep arriving, the worker sends a proactive keep-alive
        start before the firmware's measurement session expires. If the stream
        does go silent, it falls back to a reactionary re-arm after
        LIVE_REARM_IDLE seconds.
        """
        if self.live_task is not None and not self.live_task.done():
            return
        self.live_measure_type = mtype
        self.live_enabled = True
        self.live_stored = 0
        self.keepalives = 0
        self.live_error = None
        self._drain_live_queue()
        await self.send(frame(OP_MEASUREMENT_WITH_TYPE, mtype, 1))
        self.measuring = True
        self.live_task = asyncio.create_task(
            self._live_loop(mtype), name="j2208-live"
        )

    async def _live_loop(self, mtype: int) -> None:
        last_frame = time.monotonic()
        last_start = last_frame
        try:
            while True:
                try:
                    data = await asyncio.wait_for(
                        self.live_queue.get(), timeout=0.5
                    )
                except TimeoutError:
                    if time.monotonic() - last_frame >= LIVE_REARM_IDLE:
                        now = time.monotonic()
                        await self.send(frame(OP_MEASUREMENT_WITH_TYPE, mtype, 1))
                        self.rearms += 1
                        last_frame = now
                        last_start = now
                    continue
                if not data or data[0] != OP_MEASUREMENT_WITH_TYPE or len(data) < 10:
                    continue
                now = time.monotonic()
                last_frame = now
                if now - last_start >= LIVE_KEEPALIVE:
                    await self.send(frame(OP_MEASUREMENT_WITH_TYPE, mtype, 1))
                    self.keepalives += 1
                    last_start = now
                rec = decode(data)
                if has_live_metrics(rec) and await self.store.live(rec):
                    self.live_stored += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - surfaced by the run loop
            self.live_error = exc

    async def live_burst(self, mtype: int, seconds: float) -> int:
        """Run the optical sensor for a fixed `seconds`, store every reading."""
        self.live_measure_type = mtype
        self.live_enabled = True
        self._drain_live_queue()
        await self.send(frame(OP_MEASUREMENT_WITH_TYPE, mtype, 1))
        self.measuring = True

        stored = 0
        deadline = time.monotonic() + seconds
        try:
            while time.monotonic() < deadline:
                if self.stop is not None and self.stop.is_set():
                    break
                try:
                    data = await asyncio.wait_for(
                        self.live_queue.get(), timeout=0.5
                    )
                except TimeoutError:
                    continue
                if not data or data[0] != OP_MEASUREMENT_WITH_TYPE or len(data) < 10:
                    continue
                rec = decode(data)
                if has_live_metrics(rec) and await self.store.live(rec):
                    stored += 1
        finally:
            try:
                await self.send(frame(OP_MEASUREMENT_WITH_TYPE, mtype, 0))
            finally:
                self.measuring = False
                self.live_enabled = False
        return stored

    async def close(self) -> None:
        """Stop background tasks and turn the measurement off cleanly."""
        self.live_enabled = False
        tasks = (self.live_task, self.reader_task)
        self.live_task = None
        self.reader_task = None
        for task in tasks:
            if task is not None:
                task.cancel()
        for task in tasks:
            if task is not None:
                with suppress(asyncio.CancelledError):
                    await task
        if self.measuring:
            try:
                await self.send(
                    frame(OP_MEASUREMENT_WITH_TYPE, self.live_measure_type, 0)
                )
            except Exception as exc:  # noqa: BLE001 - best effort on close
                print(f"could not stop measurement: {exc}")
            self.measuring = False
        self.battery_future = None


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
                    try:
                        await s.setup()
                        backoff = 5
                        await store.event("connected")
                        print(f"[{datetime.now():%H:%M:%S}] connected")

                        live_reported = 0
                        live_rearms_reported = 0
                        live_keepalives_reported = 0
                        while not stop.is_set():
                            cycle += 1
                            t0 = time.monotonic()
                            line = [f"[{datetime.now():%H:%M:%S}] #{cycle}"]

                            pct = await s.read_battery()
                            if pct is not None:
                                await store.battery(pct, args.mode)
                                line.append(f"batt={pct}%")

                            if args.mode == "live":
                                if args.burst <= 0:
                                    if s.live_error is not None:
                                        raise s.live_error
                                    if s.live_task is None or s.live_task.done():
                                        await s.start_live(args.measure_type)
                                        live_reported = 0
                                        live_rearms_reported = s.rearms
                                        live_keepalives_reported = s.keepalives
                                    n = s.live_stored - live_reported
                                    live_reported = s.live_stored
                                    line.append(f"live+{n}")
                                    if s.keepalives > live_keepalives_reported:
                                        line.append(
                                            "keepalive+"
                                            f"{s.keepalives - live_keepalives_reported}"
                                        )
                                        live_keepalives_reported = s.keepalives
                                    if s.rearms > live_rearms_reported:
                                        line.append(
                                            f"rearm+{s.rearms - live_rearms_reported}"
                                        )
                                        live_rearms_reported = s.rearms
                                else:
                                    before_rearms = s.rearms
                                    n = await s.live_burst(
                                        args.measure_type, args.burst
                                    )
                                    line.append(f"live+{n}")
                                    if s.rearms > before_rearms:
                                        line.append(
                                            f"rearm+{s.rearms - before_rearms}"
                                        )

                            # History sync is cheap but slow; do it every Nth cycle.
                            if cycle == 1 or cycle % args.history_every == 0:
                                for htype in args.types:
                                    if stop.is_set():
                                        break
                                    try:
                                        new = await s.sync_history(htype)
                                        if new:
                                            line.append(f"{htype}+{new}")
                                    except Exception as e:  # noqa: BLE001
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
                            except TimeoutError:
                                pass
                    finally:
                        await s.close()

            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001 - keep the collector alive
                if stop.is_set():
                    break
                try:
                    await store.event("disconnected", f"{type(e).__name__}: {e}")
                except Exception as db_exc:  # noqa: BLE001
                    print(f"could not record disconnect event: {db_exc}")
                print(f"[{datetime.now():%H:%M:%S}] lost connection "
                      f"({type(e).__name__}), retry in {backoff}s")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                except TimeoutError:
                    pass
                backoff = min(backoff * 2, 120)
    finally:
        try:
            await store.event("stop")
        except Exception as exc:  # noqa: BLE001
            print(f"could not record stop event: {exc}")
        try:
            print("\n" + await store.drain_summary())
            counts = await store.stats()
        except Exception as exc:  # noqa: BLE001
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
                        "Use 0 to hold one measurement stream open across "
                        "cycles instead of restarting it every interval.")
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
