#!/usr/bin/env python3
"""Create or verify the J2208 PostgreSQL schema.

Examples:
    J2208_PG_SCHEMA=j2208 J2208_PG_USER=bob J2208_PG_PASSWORD=... \
        python tools/j2208_pg_setup.py
    python tools/j2208_pg_setup.py --check
    python tools/j2208_pg_setup.py --drop --yes

The password can be omitted when connecting through a Unix socket with peer
authentication (for example J2208_PG_HOST=/var/run/postgresql).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import psycopg
    from psycopg import sql
except ImportError:  # pragma: no cover
    raise SystemExit(
        "PostgreSQL support requires psycopg; install requirements-postgres.txt"
    )

sys.path.insert(0, str(Path(__file__).resolve().parent))
from j2208_store import PgConfig  # noqa: E402

MIGRATION = Path(__file__).resolve().parent.parent / "migrations" / "001_init.sql"


def _statements(text: str) -> list[str]:
    # Strip -- comments before splitting; a semicolon inside a comment must not
    # be mistaken for a statement boundary.
    without_comments = "\n".join(line.split("--", 1)[0] for line in text.splitlines())
    return [part.strip() for part in without_comments.split(";") if part.strip()]


def _connect(cfg: PgConfig):
    return psycopg.connect(cfg.conninfo(), autocommit=True)


def _set_schema(conn, schema: str) -> None:
    conn.execute(
        sql.SQL("SET search_path TO {}, pg_catalog").format(sql.Identifier(schema))
    )


def setup(cfg: PgConfig) -> int:
    if not MIGRATION.exists():
        print(f"migration not found: {MIGRATION}", file=sys.stderr)
        return 2
    with _connect(cfg) as conn:
        conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}").format(
                sql.Identifier(cfg.schema), sql.Identifier(cfg.user)
            )
        )
        _set_schema(conn, cfg.schema)
        for statement in _statements(MIGRATION.read_text(encoding="utf-8")):
            conn.execute(statement)
        conn.execute(
            "INSERT INTO schema_version (version) VALUES (1) "
            "ON CONFLICT (version) DO NOTHING"
        )
    print(f"schema {cfg.schema!r} is ready in database {cfg.database!r}")
    return 0


def check(cfg: PgConfig) -> int:
    with _connect(cfg) as conn:
        database, user = conn.execute(
            "SELECT current_database(), current_user"
        ).fetchone()
        print(f"database={database} user={user} schema={cfg.schema}")
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (cfg.schema,),
        ).fetchall()
        if not rows:
            print("schema has no tables (run setup first)", file=sys.stderr)
            return 1
        print("objects: " + ", ".join(row[0] for row in rows))
        _set_schema(conn, cfg.schema)
        version = conn.execute("SELECT max(version) FROM schema_version").fetchone()[0]
        print(f"schema_version: {version}")
        for table in ("devices", "history", "live", "battery", "events"):
            count = conn.execute(
                sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))
            ).fetchone()[0]
            print(f"  {table:<10} {count}")
    return 0


def drop(cfg: PgConfig, confirmed: bool) -> int:
    if not confirmed:
        print("refusing to drop without --yes", file=sys.stderr)
        return 2
    with _connect(cfg) as conn:
        conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(cfg.schema)
            )
        )
    print(f"dropped schema {cfg.schema!r}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check", action="store_true", help="verify schema and print counts"
    )
    parser.add_argument(
        "--drop", action="store_true", help="drop the schema (requires --yes)"
    )
    parser.add_argument("--yes", action="store_true", help="confirm --drop")
    args = parser.parse_args()
    try:
        cfg = PgConfig.from_env()
    except ValueError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    try:
        if args.drop:
            return drop(cfg, args.yes)
        if args.check:
            return check(cfg)
        return setup(cfg)
    except psycopg.Error as exc:
        # Do not print the connection string: it may contain the password.
        print(f"postgres error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
