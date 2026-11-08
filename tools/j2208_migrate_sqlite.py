#!/usr/bin/env python3
"""Copy an existing SQLite collector database into PostgreSQL.

The import is idempotent: history/live rows use ON CONFLICT DO NOTHING,
battery uses an upsert, and events are appended. Run it with the same
J2208_PG_* environment used by the daemon.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import psycopg
    from psycopg import sql
    from psycopg.types.json import Jsonb
except ImportError:  # pragma: no cover
    raise SystemExit(
        "PostgreSQL support requires psycopg; install requirements-postgres.txt"
    )

sys.path.insert(0, str(Path(__file__).resolve().parent))
from j2208_store import SIMPLE_VALUE_TYPES, PgConfig  # noqa: E402


def _dt(value):
    if value in (None, ""):
        return None
    return datetime.fromisoformat(str(value))


def _table(cfg: PgConfig, name: str):
    return sql.SQL("{}.{}").format(sql.Identifier(cfg.schema), sql.Identifier(name))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--sqlite", default="captures/j2208.db", help="source SQLite database"
    )
    parser.add_argument(
        "--device-id", default=None, help="device identifier for imported rows"
    )
    args = parser.parse_args()

    source_path = Path(args.sqlite)
    if not source_path.exists():
        print(f"sqlite database not found: {source_path}", file=sys.stderr)
        return 2

    cfg = PgConfig.from_env(args.device_id)
    source = sqlite3.connect(source_path)

    history_table = _table(cfg, "history")
    battery_table = _table(cfg, "battery")
    live_table = _table(cfg, "live")
    events_table = _table(cfg, "events")

    with psycopg.connect(cfg.conninfo()) as conn:
        conn.execute(
            sql.SQL("SET search_path TO {}, pg_catalog").format(
                sql.Identifier(cfg.schema)
            )
        )
        conn.execute(
            """
            INSERT INTO devices (device_id, last_seen)
            VALUES (%s, now())
            ON CONFLICT (device_id) DO UPDATE SET last_seen = now()
            """,
            (cfg.device_id,),
        )

        history_source = source.execute("SELECT count(*) FROM history").fetchone()[0]
        history_inserted = 0
        for htype, device_time, payload_json, fetched_at in source.execute(
            "SELECT type, device_time, payload, fetched_at FROM history"
        ):
            device_time = _dt(device_time)
            if device_time is None:
                continue
            payload = json.loads(payload_json)
            raw_hex = payload.pop("raw", None)
            raw = bytes.fromhex(raw_hex) if raw_hex else None
            value = payload.get("value") if htype in SIMPLE_VALUE_TYPES else None
            cur = conn.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (device_id, type, device_time, value,
                                    payload, raw, fetched_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (device_id, type, device_time) DO NOTHING
                    """
                ).format(history_table),
                (
                    cfg.device_id,
                    htype,
                    device_time,
                    value,
                    Jsonb(payload),
                    raw,
                    _dt(fetched_at) or datetime.now(timezone.utc),
                ),
            )
            history_inserted += cur.rowcount

        battery_rows = source.execute("SELECT ts, pct, mode FROM battery").fetchall()
        for ts, pct, mode in battery_rows:
            conn.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (device_id, ts, pct, mode)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (device_id, ts) DO UPDATE
                      SET pct = EXCLUDED.pct, mode = EXCLUDED.mode
                    """
                ).format(battery_table),
                (cfg.device_id, _dt(ts), pct, mode),
            )

        live_rows = source.execute(
            "SELECT ts, hr, spo2, hrv, stress, bp_sys, bp_dia, temp_c FROM live"
        ).fetchall()
        for row in live_rows:
            conn.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (device_id, ts, hr, spo2, hrv, stress,
                                    bp_sys, bp_dia, temp_c)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (device_id, ts) DO NOTHING
                    """
                ).format(live_table),
                (cfg.device_id, _dt(row[0]), *row[1:]),
            )

        event_rows = source.execute("SELECT ts, kind, detail FROM events").fetchall()
        for ts, kind, detail in event_rows:
            conn.execute(
                sql.SQL(
                    "INSERT INTO {} (device_id, ts, kind, detail) "
                    "VALUES (%s, %s, %s, %s)"
                ).format(events_table),
                (cfg.device_id, _dt(ts), kind, detail),
            )

        conn.commit()

    print(f"imported into schema {cfg.schema!r} as {cfg.device_id!r}")
    print(f"  history  {history_inserted} new / {history_source} source")
    print(f"  battery  {len(battery_rows)} source")
    print(f"  live     {len(live_rows)} source")
    print(f"  events   {len(event_rows)} source")
    source.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
