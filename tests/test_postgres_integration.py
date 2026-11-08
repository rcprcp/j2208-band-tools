"""Optional PostgreSQL integration test.

Run with a reachable database, for example:

    J2208_TEST_PG=1 J2208_PG_HOST=/var/run/postgresql \
    J2208_PG_USER=bob J2208_PG_DATABASE=bob \
    python -m unittest discover -s tests -v

The test creates and drops a uniquely named schema.
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from contextlib import suppress
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

try:
    from j2208_pg_setup import drop, setup  # noqa: E402
    from j2208_store import PgConfig, make_store  # noqa: E402
except ImportError:  # psycopg is optional
    drop = setup = PgConfig = make_store = None  # type: ignore[assignment]


@unittest.skipUnless(
    PgConfig is not None and os.environ.get("J2208_TEST_PG") == "1",
    "set J2208_TEST_PG=1 and install requirements-postgres.txt to run",
)
class PostgresStoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = f"j2208_test_{os.getpid()}"
        cls._old_schema = os.environ.get("J2208_PG_SCHEMA")
        os.environ["J2208_PG_SCHEMA"] = cls.schema
        cls.cfg = PgConfig.from_env("aa:bb:cc")
        result = setup(cls.cfg)
        if result != 0:
            raise RuntimeError(f"schema setup failed with status {result}")

    @classmethod
    def tearDownClass(cls):
        with suppress(Exception):
            drop(cls.cfg, True)
        if cls._old_schema is None:
            os.environ.pop("J2208_PG_SCHEMA", None)
        else:
            os.environ["J2208_PG_SCHEMA"] = cls._old_schema

    def test_roundtrip_and_idempotency(self):
        async def scenario():
            store = await make_store(
                backend="postgres",
                sqlite_path="unused",
                device_id="AA:BB:CC",
            )
            try:
                rows = [
                    {
                        "device_time": "2026-11-08T10:00:00",
                        "value": 60,
                        "raw": "550000",
                    },
                    {
                        "device_time": "2026-11-08T10:10:00",
                        "value": 61,
                        "raw": "550100",
                    },
                ]
                self.assertEqual(await store.history("hr", rows), 2)
                self.assertEqual(await store.history("hr", rows), 0)

                rec = {"ts": "2026-11-08T15:00:00+00:00", "hr": 70}
                self.assertTrue(await store.live(rec))
                self.assertFalse(await store.live(rec))

                await store.battery(87, "history")
                await store.event("connected", "integration test")

                stats = await store.stats()
                self.assertEqual(stats["hr"], 2)
                self.assertEqual(stats["live"], 1)
                self.assertIn("battery:", await store.drain_summary())
            finally:
                await store.close()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
