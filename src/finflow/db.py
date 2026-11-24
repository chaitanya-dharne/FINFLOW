"""Database layer: connections, schema bootstrap and all SQL used by the pipeline.

Uses plain DB-API drivers - no ORM:
  * MySQL 8.0  -> ``mysql-connector-python`` (the real target)
  * SQLite     -> Python's built-in ``sqlite3`` (zero-setup local / test mode)

SQL is written with ``:name`` placeholders; ``Conn`` converts them to the driver's own style.
Identifiers come from the trusted schema definitions, values are always bound parameters.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .config import Settings
from .errors import ConfigError
from .logging_utils import get_logger
from .schemas import ENTITIES, Entity
from .utils import chunked

log = get_logger("db")
BATCH = 2000

_PARAM = re.compile(r"(?<![:\w]):([A-Za-z_]\w*)")


def _to_pyformat(sql: str) -> str:
    """':name' -> '%(name)s' (mysql-connector style); literal '%' is escaped."""
    return _PARAM.sub(r"%(\1)s", sql.replace("%", "%%"))


# ------------------------------------------------------------------ connection wrapper
class Result:
    """Tiny cursor wrapper offering fetchall / fetchone / scalar."""

    def __init__(self, cursor):
        self._cur = cursor

    def fetchall(self) -> list[tuple]:
        return list(self._cur.fetchall())

    def fetchone(self):
        return self._cur.fetchone()

    def scalar(self):
        row = self._cur.fetchone()
        return row[0] if row else None


class Conn:
    """One open database connection (a transaction is open until commit/rollback)."""

    def __init__(self, raw, dialect_name: str):
        self.raw = raw
        self.dialect = dialect_name

    def _cursor(self):
        return self.raw.cursor()

    def execute(self, sql: str, params: dict | None = None) -> Result:
        cur = self._cursor()
        if self.dialect == "sqlite":
            cur.execute(sql, params or {})
        elif params:
            cur.execute(_to_pyformat(sql), params)
        else:  # no parameters -> send the SQL untouched (avoids '%' escaping issues)
            cur.execute(sql)
        return Result(cur)

    def executemany(self, sql: str, rows: list[dict]) -> None:
        cur = self._cursor()
        if self.dialect == "mysql":
            cur.executemany(_to_pyformat(sql), rows)
        else:
            cur.executemany(sql, rows)

    def execute_raw(self, sql: str) -> None:
        """Run a statement with no parameter handling (used for DDL scripts)."""
        self._cursor().execute(sql)

    def commit(self) -> None:
        self.raw.commit()

    def rollback(self) -> None:
        self.raw.rollback()

    def close(self) -> None:
        self.raw.close()


class Database:
    """Connection factory. ``connect()`` opens a new connection every time."""

    def __init__(self, dialect_name: str, opener, label: str):
        if dialect_name not in {"mysql", "sqlite"}:
            raise ConfigError(f"Unsupported database dialect: {dialect_name}")
        self.dialect = dialect_name
        self._opener = opener
        self.label = label

    def connect(self) -> Conn:
        return Conn(self._opener(), self.dialect)


def make_database(settings: Settings) -> Database:
    """Build a Database handle for the configured backend."""
    log.info("creating database handle", extra={"url": settings.safe_database_url()})
    if settings.db_backend == "sqlite":
        settings.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        path = str(settings.sqlite_path)

        def open_sqlite():
            raw = sqlite3.connect(path)
            raw.execute("PRAGMA foreign_keys = ON")
            return raw

        return Database("sqlite", open_sqlite, settings.safe_database_url())

    password = settings.mysql_password()

    def open_mysql():
        try:
            import mysql.connector
        except ImportError as exc:  # pragma: no cover
            raise ConfigError("Install mysql-connector-python (pip install -r requirements.txt)") from exc
        return mysql.connector.connect(
            host=settings.mysql_host, port=settings.mysql_port, user=settings.mysql_user,
            password=password, database=settings.mysql_database, charset="utf8mb4",
            autocommit=False, connection_timeout=10,
        )

    return Database("mysql", open_mysql, settings.safe_database_url())


TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (sqlite3.OperationalError,)
try:  # add the MySQL driver's connection errors when the driver is installed
    import mysql.connector as _mc

    TRANSIENT_ERRORS += (_mc.errors.OperationalError, _mc.errors.InterfaceError)
except ImportError:  # pragma: no cover
    pass


@contextmanager
def begin(db: Database) -> Iterator[Conn]:
    """Transaction scope: commit on success, rollback on any exception."""
    conn = db.connect()
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def connect(db: Database) -> Iterator[Conn]:
    """Read-only style connection scope (always rolled back / closed)."""
    conn = db.connect()
    try:
        yield conn
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


# ------------------------------------------------------------------ schema bootstrap
def _split_statements(sql: str) -> list[str]:
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    cleaned = "\n".join(re.sub(r"\s--.*$", "", ln) for ln in lines)
    return [s.strip() for s in cleaned.split(";") if s.strip()]


def schema_files(sql_dir: Path, dialect_name: str) -> list[Path]:
    if dialect_name == "sqlite":
        return [sql_dir / "sqlite" / "schema.sql"]
    return [sql_dir / "mysql" / n for n in (
        "02_audit_tables.sql", "03_staging_tables.sql", "04_curated_tables.sql", "05_views.sql")]


def init_schema(db: Database, sql_dir: Path) -> None:
    """Idempotent DDL bootstrap (CREATE ... IF NOT EXISTS)."""
    for path in schema_files(sql_dir, db.dialect):
        statements = _split_statements(path.read_text(encoding="utf-8"))
        with begin(db) as conn:
            for stmt in statements:
                conn.execute_raw(stmt)
        log.info("schema script applied", extra={"file": path.name, "statements": len(statements)})


# ------------------------------------------------------------------ audit helpers
def audit_start(db: Database, load_id: str, source_dir: str, started: str, dry_run: bool) -> None:
    with begin(db) as conn:
        conn.execute(
            "INSERT INTO etl_load_audit (load_id, status, source_dir, started_at) "
            "VALUES (:l, :s, :d, :t)",
            {"l": load_id, "s": "RUNNING", "d": source_dir, "t": started},
        )


def audit_finish(db: Database, load_id: str, status: str, finished: str, *, error: str | None = None) -> None:
    with begin(db) as conn:
        conn.execute(
            "UPDATE etl_load_audit SET status=:s, finished_at=:f, error_message=:e WHERE load_id=:l",
            {"s": status, "f": finished, "e": error, "l": load_id},
        )


def registered_hashes(conn: Conn, entity: str) -> set[str]:
    rows = conn.execute(
        "SELECT sha256 FROM etl_file_registry WHERE entity=:e AND status='LOADED'", {"e": entity}
    ).fetchall()
    return {r[0] for r in rows}


def fetch_watermark(conn: Conn, entity: str) -> str | None:
    row = conn.execute(
        "SELECT watermark_value FROM etl_watermark WHERE entity=:e", {"e": entity}
    ).fetchone()
    return str(row[0])[:19].replace("T", " ") if row else None


def existing_keys(conn: Conn, spec: Entity) -> set[str]:
    return {r[0] for r in conn.execute(f"SELECT {spec.pk} FROM {spec.table}").fetchall()}


def existing_account_currency(conn: Conn) -> dict[str, str]:
    return {r[0]: r[1] for r in conn.execute("SELECT account_id, currency FROM accounts").fetchall()}


# ------------------------------------------------------------------ staging + merge
def stage_rows(conn: Conn, spec: Entity, records: list[dict], load_id: str, source_file: str, now: str) -> None:
    """Replace this load's staging rows (rerun-safe) and insert the validated records."""
    conn.execute(f"DELETE FROM {spec.stg_table} WHERE load_id = :l", {"l": load_id})
    if not records:
        return
    cols = spec.column_names + ["row_hash"]
    all_cols = ["load_id", *cols, "source_file", "source_row_number", "staged_at"]
    stmt = (f"INSERT INTO {spec.stg_table} ({', '.join(all_cols)}) "
            f"VALUES ({', '.join(':' + c for c in all_cols)})")
    for part in chunked(records, BATCH):
        payload = [{**r, "load_id": load_id, "source_file": source_file, "staged_at": now} for r in part]
        conn.executemany(stmt, payload)


@dataclass
class MergeCounts:
    staged: int
    inserted: int
    updated: int
    unchanged: int


def merge_counts(conn: Conn, spec: Entity, load_id: str) -> MergeCounts:
    row = conn.execute(
        f"SELECT COUNT(*), "
        f"COALESCE(SUM(CASE WHEN c.{spec.pk} IS NULL THEN 1 ELSE 0 END), 0), "
        f"COALESCE(SUM(CASE WHEN c.{spec.pk} IS NOT NULL AND c.row_hash <> s.row_hash THEN 1 ELSE 0 END), 0) "
        f"FROM {spec.stg_table} s LEFT JOIN {spec.table} c ON c.{spec.pk} = s.{spec.pk} "
        f"WHERE s.load_id = :l",
        {"l": load_id},
    ).fetchone()
    staged, inserted, updated = int(row[0]), int(row[1]), int(row[2])
    return MergeCounts(staged, inserted, updated, staged - inserted - updated)


def merge_sql(dialect_name: str, spec: Entity) -> str:
    """Staging -> curated upsert. Only new or changed rows (row_hash differs) are written."""
    cols = spec.column_names
    insert_cols = [*cols, "row_hash", "source_file", "created_load_id", "updated_load_id",
                   "etl_created_at", "etl_updated_at"]
    select_exprs = [f"s.{c}" for c in cols] + [
        "s.row_hash", "s.source_file", "s.load_id", "s.load_id", ":now", ":now"]
    body = (
        f"INSERT INTO {spec.table} ({', '.join(insert_cols)}) "
        f"SELECT {', '.join(select_exprs)} FROM {spec.stg_table} s "
        f"LEFT JOIN {spec.table} c ON c.{spec.pk} = s.{spec.pk} "
        f"WHERE s.load_id = :load_id AND (c.{spec.pk} IS NULL OR c.row_hash <> s.row_hash) "
    )
    updatable = [c for c in cols if c != spec.pk] + ["row_hash", "source_file", "updated_load_id", "etl_updated_at"]
    if dialect_name == "mysql":
        sets = ", ".join(f"{c} = VALUES({c})" for c in updatable)
        return body + f"ON DUPLICATE KEY UPDATE {sets}"
    sets = ", ".join(f"{c} = excluded.{c}" for c in updatable)
    return body + f"ON CONFLICT ({spec.pk}) DO UPDATE SET {sets}"


def merge_to_curated(conn: Conn, spec: Entity, load_id: str, now: str) -> None:
    conn.execute(merge_sql(conn.dialect, spec), {"load_id": load_id, "now": now})


def purge_staging(conn: Conn, keep_loads: int) -> None:
    """Keep staging rows only for the most recent N loads."""
    keep = (
        "SELECT load_id FROM (SELECT load_id FROM etl_load_audit "
        f"ORDER BY started_at DESC, load_id DESC LIMIT {int(keep_loads)}) recent"
    )
    for spec in ENTITIES:
        conn.execute(f"DELETE FROM {spec.stg_table} WHERE load_id NOT IN ({keep})")


# ------------------------------------------------------------------ diagnostics persistence
def persist_rejects(conn: Conn, load_id: str, source_file_by_entity: dict[str, str], rejects: list[dict], now: str) -> None:
    stmt = (
        "INSERT INTO etl_rejected_records (load_id, entity, source_file, source_row_number, record_key, "
        "rule_codes, messages, raw_record, rejected_at) VALUES "
        "(:load_id, :entity, :source_file, :source_row_number, :record_key, :rule_codes, :messages, "
        ":raw_record, :now)"
    )
    for part in chunked(rejects, BATCH):
        conn.executemany(stmt, [
            {**r, "load_id": load_id, "source_file": source_file_by_entity[r["entity"]], "now": now}
            for r in part])


def persist_dq(conn: Conn, load_id: str, stats: list[dict], now: str) -> None:
    if stats:
        conn.executemany(
            "INSERT INTO etl_dq_results (load_id, entity, rule_code, severity, failed_rows, "
            "checked_rows, checked_at) VALUES (:load_id, :entity, :rule_code, :severity, "
            ":failed_rows, :checked_rows, :now)",
            [{**s, "load_id": load_id, "now": now} for s in stats])


def persist_recon(conn: Conn, load_id: str, checks: list[dict], now: str) -> None:
    if checks:
        conn.executemany(
            "INSERT INTO etl_reconciliation (load_id, entity, check_name, source_value, target_value, "
            "difference, status, checked_at) VALUES (:load_id, :entity, :check_name, :source_value, "
            ":target_value, :difference, :status, :now)",
            [{**c, "load_id": load_id, "now": now} for c in checks])


def register_file(conn: Conn, entity: str, file_name: str, sha256: str, size: int, rows: int, load_id: str, now: str) -> None:
    exists = conn.execute(
        "SELECT COUNT(*) FROM etl_file_registry WHERE entity=:e AND sha256=:h",
        {"e": entity, "h": sha256}).scalar()
    params = {"e": entity, "n": file_name, "h": sha256, "z": size, "r": rows, "l": load_id, "t": now}
    if exists:
        conn.execute("UPDATE etl_file_registry SET file_name=:n, size_bytes=:z, row_count=:r, "
                     "load_id=:l, status='LOADED', registered_at=:t WHERE entity=:e AND sha256=:h", params)
    else:
        conn.execute("INSERT INTO etl_file_registry (entity, file_name, sha256, size_bytes, row_count, "
                     "load_id, status, registered_at) VALUES (:e, :n, :h, :z, :r, :l, 'LOADED', :t)", params)


def advance_watermark(conn: Conn, entity: str, value: str | None, load_id: str, now: str) -> None:
    if not value:
        return
    current = fetch_watermark(conn, entity)
    if current is None:
        conn.execute("INSERT INTO etl_watermark (entity, watermark_value, last_load_id, updated_at) "
                     "VALUES (:e, :v, :l, :t)", {"e": entity, "v": value, "l": load_id, "t": now})
    elif value > current:
        conn.execute("UPDATE etl_watermark SET watermark_value=:v, last_load_id=:l, updated_at=:t "
                     "WHERE entity=:e", {"e": entity, "v": value, "l": load_id, "t": now})


def recent_loads(conn: Conn, limit: int = 10) -> list[tuple]:
    return conn.execute(
        "SELECT load_id, status, started_at, finished_at, error_message "
        f"FROM etl_load_audit ORDER BY started_at DESC LIMIT {int(limit)}").fetchall()
