"""Storage backends for the J2208 collector.

SQLite remains the zero-dependency default. PostgreSQL is optional and is
selected with ``J2208_DB_BACKEND=postgres`` or ``--backend postgres``.

PostgreSQL environment:
    J2208_PG_SCHEMA    schema name (default: j2208)
    J2208_PG_USER      role (fallback: PGUSER, $USER)
    J2208_PG_PASSWORD  password (fallback: PGPASSWORD; omit for peer auth)
    J2208_PG_HOST      host (fallback: PGHOST; omit for the libpq default)
    J2208_PG_PORT      port (fallback: PGPORT; default: 5432)
    J2208_PG_DATABASE  database (fallback: PGDATABASE; default: user)
    J2208_PG_SSLMODE   sslmode (fallback: PGSSLMODE; default: prefer)
    J2208_PG_POOL_MIN  minimum pool size (default: 1)
    J2208_PG_POOL_MAX  maximum pool size (default: 4)
    J2208_DEVICE_ID    device identifier (default: default)

Table names are always schema-qualified. The schema name is validated as a
plain identifier and composed with psycopg.sql.Identifier, never interpolated.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

try:  # PostgreSQL is optional; SQLite users must not need psycopg installed.
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    from psycopg.types.json import Jsonb
    from psycopg_pool import AsyncConnectionPool
except ImportError:  # pragma: no cover - exercised only without the extra
    sql = None  # type: ignore[assignment]
    make_conninfo = None  # type: ignore[assignment]
    Jsonb = None  # type: ignore[assignment]
    AsyncConnectionPool = None  # type: ignore[assignment]


IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
RESERVED_SCHEMAS = {"information_schema", "pg_catalog", "pg_toast", "public"}
SIMPLE_VALUE_TYPES = {"hr", "spo2_auto", "temp_axil", "spo2", "temp"}


def validate_schema(name: str) -> str:
    """Validate a schema name before it reaches SQL.

    Identifiers cannot be bound as query parameters, so the only safe options
    are strict validation plus psycopg.sql.Identifier. Both are used here.
    """
    if not name or not IDENT_RE.fullmatch(name):
        raise ValueError(
            f"schema name must match ^[a-z_][a-z0-9_]{{0,62}}$ (got {name!r})"
        )
    if name.startswith("pg_") or name in RESERVED_SCHEMAS:
        raise ValueError(f"schema name {name!r} is reserved")
    return name


def normalise_device_id(value: str) -> str:
    return value.strip().upper()


def _env_first(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value not in (None, ""):
            return value
    return default


@dataclass(frozen=True)
class PgConfig:
    schema: str
    user: str
    password: str | None
    host: str | None
    port: int
    database: str
    sslmode: str
    pool_min: int
    pool_max: int
    device_id: str

    @classmethod
    def from_env(cls, device_id: str | None = None) -> "PgConfig":
        user = (
            _env_first(
                "J2208_PG_USER", "PGUSER", default=os.environ.get("USER", "postgres")
            )
            or "postgres"
        )
        schema = validate_schema(
            _env_first("J2208_PG_SCHEMA", default="j2208") or "j2208"
        )
        database = _env_first("J2208_PG_DATABASE", "PGDATABASE", default=user) or user
        host = _env_first("J2208_PG_HOST", "PGHOST")
        password = _env_first("J2208_PG_PASSWORD", "PGPASSWORD")
        port = int(_env_first("J2208_PG_PORT", "PGPORT", default="5432") or "5432")
        sslmode = (
            _env_first("J2208_PG_SSLMODE", "PGSSLMODE", default="prefer") or "prefer"
        )
        pool_min = int(_env_first("J2208_PG_POOL_MIN", default="1") or "1")
        pool_max = int(_env_first("J2208_PG_POOL_MAX", default="4") or "4")
        if pool_min < 0 or pool_max < 1 or pool_min > pool_max:
            raise ValueError(
                "J2208_PG_POOL_MIN/MAX must satisfy 0 <= MIN <= MAX and MAX >= 1"
            )

        selected = (
            device_id or _env_first("J2208_DEVICE_ID", default="default") or "default"
        )
        return cls(
            schema=schema,
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
            sslmode=sslmode,
            pool_min=pool_min,
            pool_max=pool_max,
            device_id=normalise_device_id(selected),
        )

    def conninfo(self) -> str:
        if make_conninfo is None:
            raise RuntimeError(
                "PostgreSQL support requires psycopg; install requirements-postgres.txt"
            )
        kwargs: dict[str, Any] = {
            "user": self.user,
            "dbname": self.database,
            "port": self.port,
            "sslmode": self.sslmode,
            "application_name": "j2208-daemon",
            "options": "-c timezone=UTC",
        }
        if self.host:
            kwargs["host"] = self.host
        if self.password:
            kwargs["password"] = self.password
        return make_conninfo(**kwargs)


def _as_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _summary(rows: list[tuple[Any, int]]) -> str:
    if len(rows) < 2:
        return "battery: not enough samples yet"
    t0 = _as_datetime(rows[0][0])
    t1 = _as_datetime(rows[-1][0])
    if t0 is None or t1 is None:
        return "battery: not enough samples yet"
    hours = (t1 - t0).total_seconds() / 3600
    drop = rows[0][1] - rows[-1][1]
    if hours < 0.5:
        return f"battery: {rows[-1][1]}% (need ~1h for a drain estimate)"
    rate = drop / hours
    life = f"{100 / rate:.0f}h to flat" if rate > 0 else "no measurable drop yet"
    return (
        f"battery: {rows[0][1]}% -> {rows[-1][1]}% over {hours:.1f}h "
        f"({rate:+.2f} %/h, {life})"
    )


class Store(Protocol):
    async def event(self, kind: str, detail: str = "") -> None: ...
    async def battery(self, pct: int, mode: str) -> None: ...
    async def live(self, rec: dict) -> bool: ...
    async def history(self, htype: str, rows: list[dict]) -> int: ...
    async def drain_summary(self) -> str: ...
    async def stats(self) -> dict[str, int]: ...
    async def ensure_device(self, name: str | None = None) -> None: ...
    async def close(self) -> None: ...


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS live (
    ts TEXT NOT NULL, hr INTEGER, spo2 INTEGER, hrv INTEGER, stress INTEGER,
    bp_sys INTEGER, bp_dia INTEGER, temp_c REAL,
    PRIMARY KEY (ts)
);
CREATE TABLE IF NOT EXISTS battery (
    ts TEXT PRIMARY KEY, pct INTEGER NOT NULL, mode TEXT
);
CREATE TABLE IF NOT EXISTS history (
    type TEXT NOT NULL, device_time TEXT NOT NULL, payload TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (type, device_time)
);
CREATE TABLE IF NOT EXISTS events (
    ts TEXT NOT NULL, kind TEXT NOT NULL, detail TEXT
);
"""


class SqliteStore:
    """Existing single-file backend, kept for portability and offline use."""

    backend = "sqlite"

    device_id = "default"

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.executescript(SQLITE_SCHEMA)
        self.db.commit()

    async def ensure_device(self, name: str | None = None) -> None:
        return None

    async def event(self, kind: str, detail: str = "") -> None:
        self.db.execute(
            "INSERT INTO events VALUES (?,?,?)",
            (datetime.now(timezone.utc).isoformat(), kind, detail),
        )
        self.db.commit()

    async def battery(self, pct: int, mode: str) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO battery VALUES (?,?,?)",
            (datetime.now(timezone.utc).isoformat(), pct, mode),
        )
        self.db.commit()

    async def live(self, rec: dict) -> bool:
        cur = self.db.execute(
            "INSERT OR IGNORE INTO live VALUES (?,?,?,?,?,?,?,?)",
            (
                rec["ts"],
                rec.get("hr"),
                rec.get("spo2"),
                rec.get("hrv"),
                rec.get("stress"),
                rec.get("bp_sys"),
                rec.get("bp_dia"),
                rec.get("temp_c"),
            ),
        )
        self.db.commit()
        return cur.rowcount > 0

    async def history(self, htype: str, rows: list[dict]) -> int:
        new = 0
        stamp = datetime.now(timezone.utc).isoformat()
        for row in rows:
            device_time = row.get("device_time")
            if not device_time:
                continue
            cur = self.db.execute(
                "INSERT OR IGNORE INTO history VALUES (?,?,?,?)",
                (htype, device_time, json.dumps(row), stamp),
            )
            new += cur.rowcount
        self.db.commit()
        return new

    async def drain_summary(self) -> str:
        rows = self.db.execute("SELECT ts, pct FROM battery ORDER BY ts").fetchall()
        return _summary(rows)

    async def stats(self) -> dict[str, int]:
        counts = {
            htype: count
            for htype, count in self.db.execute(
                "SELECT type, COUNT(*) FROM history GROUP BY type ORDER BY type"
            )
        }
        counts["live"] = self.db.execute("SELECT COUNT(*) FROM live").fetchone()[0]
        return counts

    async def close(self) -> None:
        self.db.close()


class PostgresStore:
    """Optional PostgreSQL backend using an async psycopg connection pool."""

    backend = "postgres"

    def __init__(self, cfg: PgConfig):
        if AsyncConnectionPool is None or sql is None or Jsonb is None:
            raise RuntimeError(
                "PostgreSQL support requires psycopg and psycopg-pool; "
                "install requirements-postgres.txt"
            )
        self.cfg = cfg
        self.device_id = cfg.device_id
        self.pool = AsyncConnectionPool(
            cfg.conninfo(),
            min_size=cfg.pool_min,
            max_size=cfg.pool_max,
            open=False,
            configure=self._configure,
        )
        self._table = {
            name: sql.SQL("{}.{}").format(
                sql.Identifier(cfg.schema), sql.Identifier(name)
            )
            for name in ("devices", "history", "live", "battery", "events")
        }

    async def _configure(self, conn) -> None:
        await conn.execute(
            sql.SQL("SET search_path TO {}, pg_catalog").format(
                sql.Identifier(self.cfg.schema)
            )
        )
        await conn.execute("SET TIME ZONE 'UTC'")
        # The pool requires the connection to be idle, not left in a
        # transaction by SET.
        await conn.commit()

    async def open(self) -> None:
        await self.pool.open()
        await self.pool.wait(timeout=15)

    async def close(self) -> None:
        with suppress(Exception):
            await self.pool.close()

    async def ensure_device(self, name: str | None = None) -> None:
        query = sql.SQL(
            """
            INSERT INTO {} (device_id, advertised_name, last_seen)
            VALUES (%s, %s, now())
            ON CONFLICT (device_id) DO UPDATE
              SET last_seen = now(),
                  advertised_name = COALESCE(EXCLUDED.advertised_name,
                                             devices.advertised_name)
            """
        ).format(self._table["devices"])
        async with self.pool.connection() as conn:
            await conn.execute(query, (self.cfg.device_id, name))

    async def event(self, kind: str, detail: str = "") -> None:
        query = sql.SQL(
            "INSERT INTO {} (device_id, kind, detail) VALUES (%s, %s, %s)"
        ).format(self._table["events"])
        async with self.pool.connection() as conn:
            await conn.execute(query, (self.cfg.device_id, kind, detail))

    async def battery(self, pct: int, mode: str) -> None:
        query = sql.SQL(
            """
            INSERT INTO {} (device_id, ts, pct, mode)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (device_id, ts) DO UPDATE
              SET pct = EXCLUDED.pct, mode = EXCLUDED.mode
            """
        ).format(self._table["battery"])
        async with self.pool.connection() as conn:
            await conn.execute(
                query,
                (self.cfg.device_id, datetime.now(timezone.utc), pct, mode),
            )

    async def live(self, rec: dict) -> bool:
        query = sql.SQL(
            """
            INSERT INTO {} (device_id, ts, hr, spo2, hrv, stress,
                            bp_sys, bp_dia, temp_c)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id, ts) DO NOTHING
            """
        ).format(self._table["live"])
        ts = _as_datetime(rec.get("ts")) or datetime.now(timezone.utc)
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                query,
                (
                    self.cfg.device_id,
                    ts,
                    rec.get("hr"),
                    rec.get("spo2"),
                    rec.get("hrv"),
                    rec.get("stress"),
                    rec.get("bp_sys"),
                    rec.get("bp_dia"),
                    rec.get("temp_c"),
                ),
            )
            return cur.rowcount > 0

    async def history(self, htype: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        query = sql.SQL(
            """
            INSERT INTO {} (device_id, type, device_time, value, payload,
                            raw, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id, type, device_time) DO NOTHING
            """
        ).format(self._table["history"])
        fetched_at = datetime.now(timezone.utc)
        inserted = 0
        async with self.pool.connection() as conn:
            async with conn.transaction():
                for row in rows:
                    device_time = _as_datetime(row.get("device_time"))
                    if device_time is None:
                        continue
                    payload = {k: v for k, v in row.items() if k != "raw"}
                    raw_hex = row.get("raw")
                    raw = bytes.fromhex(raw_hex) if raw_hex else None
                    value = row.get("value") if htype in SIMPLE_VALUE_TYPES else None
                    cur = await conn.execute(
                        query,
                        (
                            self.cfg.device_id,
                            htype,
                            device_time,
                            value,
                            Jsonb(payload),
                            raw,
                            fetched_at,
                        ),
                    )
                    inserted += cur.rowcount
        return inserted

    async def drain_summary(self) -> str:
        query = sql.SQL(
            "SELECT ts, pct FROM {} WHERE device_id = %s ORDER BY ts"
        ).format(self._table["battery"])
        async with self.pool.connection() as conn:
            cur = await conn.execute(query, (self.cfg.device_id,))
            rows = await cur.fetchall()
        return _summary(list(rows))

    async def stats(self) -> dict[str, int]:
        history_query = sql.SQL(
            "SELECT type, COUNT(*) FROM {} WHERE device_id = %s "
            "GROUP BY type ORDER BY type"
        ).format(self._table["history"])
        live_query = sql.SQL("SELECT COUNT(*) FROM {} WHERE device_id = %s").format(
            self._table["live"]
        )
        async with self.pool.connection() as conn:
            cur = await conn.execute(history_query, (self.cfg.device_id,))
            rows = await cur.fetchall()
            cur = await conn.execute(live_query, (self.cfg.device_id,))
            live_count = (await cur.fetchone())[0]
        counts = {htype: count for htype, count in rows}
        counts["live"] = live_count
        return counts


def backend_from_env(default: str = "sqlite") -> str:
    return (_env_first("J2208_DB_BACKEND", default=default) or default).lower()


async def make_store(
    *,
    backend: str,
    sqlite_path: str | Path,
    device_id: str,
    pg_config: PgConfig | None = None,
) -> Store:
    backend = (backend or "sqlite").lower()
    if backend == "postgres":
        store = PostgresStore(pg_config or PgConfig.from_env(device_id))
        try:
            await store.open()
            await store.ensure_device()
        except Exception:
            await store.close()
            raise
        return store
    if backend == "sqlite":
        return SqliteStore(Path(sqlite_path))
    raise ValueError(f"unknown backend {backend!r} (expected sqlite or postgres)")
