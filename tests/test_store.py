"""Unit tests for the storage layer (no PostgreSQL required)."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

from j2208_store import (  # noqa: E402
    PgConfig,
    SqliteStore,
    normalise_device_id,
    validate_schema,
)


class SchemaValidationTests(unittest.TestCase):
    def test_accepts_safe_identifiers(self):
        for name in ("j2208", "j2208_test_01", "_private", "a" * 63):
            self.assertEqual(validate_schema(name), name)

    def test_rejects_unsafe_or_reserved_identifiers(self):
        for name in (
            "",
            "J2208",
            "1j2208",
            "j-2208",
            "j2208;drop schema public",
            "pg_temp",
            "pg_catalog",
            "information_schema",
            "public",
            "a" * 64,
        ):
            with self.assertRaises(ValueError, msg=name):
                validate_schema(name)

    def test_device_id_is_normalised(self):
        self.assertEqual(
            normalise_device_id(" d0:85:55:0a:74:b8 "),
            "D0:85:55:0A:74:B8",
        )


class ConfigTests(unittest.TestCase):
    def test_defaults(self):
        with patch.dict(os.environ, {"USER": "bob"}, clear=True):
            cfg = PgConfig.from_env("aa:bb")
        self.assertEqual(cfg.schema, "j2208")
        self.assertEqual(cfg.user, "bob")
        self.assertEqual(cfg.database, "bob")
        self.assertEqual(cfg.port, 5432)
        self.assertEqual(cfg.device_id, "AA:BB")
        self.assertIsNone(cfg.password)
        self.assertEqual(cfg.pool_min, 1)
        self.assertEqual(cfg.pool_max, 4)

    def test_custom_values_and_pg_fallbacks(self):
        env = {
            "J2208_PG_SCHEMA": "band",
            "J2208_PG_USER": "collector",
            "J2208_PG_PASSWORD": "secret",
            "J2208_PG_HOST": "127.0.0.1",
            "J2208_PG_PORT": "5544",
            "J2208_PG_DATABASE": "health",
            "J2208_PG_SSLMODE": "require",
            "J2208_PG_POOL_MIN": "0",
            "J2208_PG_POOL_MAX": "2",
            "J2208_DEVICE_ID": "aa:bb:cc",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = PgConfig.from_env()
        self.assertEqual(cfg.schema, "band")
        self.assertEqual(cfg.user, "collector")
        self.assertEqual(cfg.password, "secret")
        self.assertEqual(cfg.host, "127.0.0.1")
        self.assertEqual(cfg.port, 5544)
        self.assertEqual(cfg.database, "health")
        self.assertEqual(cfg.sslmode, "require")
        self.assertEqual(cfg.pool_min, 0)
        self.assertEqual(cfg.pool_max, 2)
        self.assertEqual(cfg.device_id, "AA:BB:CC")

    def test_pg_fallbacks(self):
        env = {
            "PGUSER": "pgbob",
            "PGPASSWORD": "pgsecret",
            "PGHOST": "/var/run/postgresql",
            "PGPORT": "5433",
            "PGDATABASE": "pgdb",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = PgConfig.from_env()
        self.assertEqual(cfg.user, "pgbob")
        self.assertEqual(cfg.password, "pgsecret")
        self.assertEqual(cfg.host, "/var/run/postgresql")
        self.assertEqual(cfg.port, 5433)
        self.assertEqual(cfg.database, "pgdb")

    def test_rejects_bad_pool_sizes(self):
        with patch.dict(
            os.environ, {"J2208_PG_POOL_MIN": "3", "J2208_PG_POOL_MAX": "2"}, clear=True
        ):
            with self.assertRaises(ValueError):
                PgConfig.from_env()


class SqliteStoreTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SqliteStore(Path(self.tempdir.name) / "test.db")

    def tearDown(self):
        asyncio.run(self.store.close())
        self.tempdir.cleanup()

    def test_history_is_idempotent_and_counted(self):
        rows = [
            {"device_time": "2026-11-08T10:00:00", "value": 60, "raw": "aa"},
            {"device_time": "2026-11-08T10:10:00", "value": 61, "raw": "bb"},
        ]
        self.assertEqual(asyncio.run(self.store.history("hr", rows)), 2)
        self.assertEqual(asyncio.run(self.store.history("hr", rows)), 0)
        self.assertEqual(asyncio.run(self.store.stats())["hr"], 2)

    def test_live_deduplicates(self):
        rec = {"ts": "2026-11-08T15:00:00+00:00", "hr": 70}
        self.assertTrue(asyncio.run(self.store.live(rec)))
        self.assertFalse(asyncio.run(self.store.live(rec)))
        self.assertEqual(asyncio.run(self.store.stats())["live"], 1)

    def test_battery_and_summary(self):
        asyncio.run(self.store.battery(90, "history"))
        asyncio.run(self.store.battery(89, "history"))
        self.assertEqual(asyncio.run(self.store.stats())["live"], 0)
        summary = asyncio.run(self.store.drain_summary())
        self.assertIn("battery:", summary)

    def test_events_are_recorded(self):
        asyncio.run(self.store.event("connected", "test"))
        asyncio.run(self.store.event("stop"))
        rows = self.store.db.execute(
            "SELECT kind, detail FROM events ORDER BY rowid"
        ).fetchall()
        self.assertEqual(rows, [("connected", "test"), ("stop", "")])


if __name__ == "__main__":
    unittest.main()
