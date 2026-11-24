"""Pipeline orchestrator: raw -> validate -> staging -> curated, with reconciliation.

Guarantees
- Raw files are snapshotted (checksummed) before anything else.
- Validation never writes to the database.
- Staging, curated merge, reconciliation, rejects, DQ results, file registry and watermarks are
  written in ONE transaction. A failed reconciliation or any error rolls everything back.
- Re-running the same files is a no-op (file registry); forcing a rerun is still safe (hash-based merge).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import db
from .config import Settings
from .errors import DataQualityError, ReconciliationError
from .extract import SourceFile, discover, read_source, snapshot_to_raw
from .logging_utils import get_logger, set_load_id
from .quality import References, ValidationResult, validate_entity
from .reconciliation import ReconCheck, reconcile_entity, referential_checks
from .reports import write_reports
from .schemas import ENTITIES, Entity
from .transform import add_row_hash, apply_watermark, max_watermark, watermark_cutoff
from .utils import fmt_ts, new_load_id, retry, to_records, utc_now

log = get_logger("pipeline")


class _DryRunRollback(Exception):
    """Raised on purpose to roll back a dry run after all checks have executed."""


@dataclass
class EntityOutcome:
    entity: str
    source_rows: int = 0
    rejected_rows: int = 0
    skipped_watermark: int = 0
    staged_rows: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped_duplicate_file: bool = False


@dataclass
class RunContext:
    settings: Settings
    load_id: str
    source_dir: Path
    dry_run: bool
    started_at: datetime
    finished_at: datetime | None = None
    status: str = "RUNNING"
    error: str | None = None
    files: dict[str, SourceFile] = field(default_factory=dict)
    outcomes: dict[str, EntityOutcome] = field(default_factory=dict)
    validations: dict[str, ValidationResult] = field(default_factory=dict)
    staged: dict[str, pd.DataFrame] = field(default_factory=dict)
    watermarks: dict[str, str | None] = field(default_factory=dict)
    recon: list[ReconCheck] = field(default_factory=list)
    reports_dir: Path | None = None

    def summary(self) -> dict:
        return {
            "load_id": self.load_id,
            "status": self.status,
            "dry_run": self.dry_run,
            "started_at": fmt_ts(self.started_at),
            "finished_at": fmt_ts(self.finished_at) if self.finished_at else None,
            "source_dir": str(self.source_dir),
            "recon_passed": sum(c.status == "PASS" for c in self.recon),
            "recon_failed": sum(c.status == "FAIL" for c in self.recon),
            "error": self.error,
            "entities": {k: vars(v) for k, v in self.outcomes.items()},
        }


def run_pipeline(
    settings: Settings,
    source_dir: Path,
    *,
    force: bool = False,
    dry_run: bool = False,
    full_refresh: bool = False,
    database=None,
) -> RunContext:
    load_id = new_load_id()
    set_load_id(load_id)
    database = database or db.make_database(settings)
    if settings.auto_init_schema:
        db.init_schema(database, settings.sql_dir)
    ctx = RunContext(settings, load_id, Path(source_dir), dry_run, utc_now())
    db.audit_start(database, load_id, str(ctx.source_dir), fmt_ts(ctx.started_at), dry_run)
    log.info("load started", extra={"source_dir": str(ctx.source_dir), "dry_run": dry_run, "force": force})
    try:
        _execute(ctx, database, force=force, full_refresh=full_refresh)
        ctx.status = "DRY_RUN" if dry_run else ("SKIPPED" if _all_skipped(ctx) else "SUCCESS")
    except Exception as exc:
        ctx.status, ctx.error = "FAILED", f"{type(exc).__name__}: {exc}"[:2000]
        log.error("load failed", extra={"error": ctx.error}, exc_info=True)
        _best_effort_diagnostics(ctx, database)
        ctx.finished_at = utc_now()
        ctx.reports_dir = write_reports(ctx, settings.reports_dir / load_id)
        db.audit_finish(database, load_id, "FAILED", fmt_ts(ctx.finished_at), error=ctx.error)
        raise
    ctx.finished_at = utc_now()
    ctx.reports_dir = write_reports(ctx, settings.reports_dir / load_id)
    db.audit_finish(database, load_id, ctx.status, fmt_ts(ctx.finished_at))
    log.info("load finished", extra=ctx.summary() | {"entities": None})
    return ctx


def _all_skipped(ctx: RunContext) -> bool:
    return bool(ctx.outcomes) and all(o.skipped_duplicate_file for o in ctx.outcomes.values())


def _execute(ctx: RunContext, database, *, force: bool, full_refresh: bool) -> None:
    s = ctx.settings
    now_dt = ctx.started_at

    # 1. EXTRACT -> immutable raw layer
    sources = discover(ctx.source_dir, ENTITIES)
    ctx.files = snapshot_to_raw(sources, s.raw_dir, ctx.load_id)

    # 2. Duplicate-file prevention + reference data + watermarks (read-only)
    refs = References()
    with db.connect(database) as conn:
        to_process: list[Entity] = []
        for spec in ENTITIES:
            outcome = ctx.outcomes.setdefault(spec.name, EntityOutcome(spec.name))
            if not force and ctx.files[spec.name].sha256 in db.registered_hashes(conn, spec.name):
                outcome.skipped_duplicate_file = True
                log.info("file already loaded; skipping", extra={"entity": spec.name})
            else:
                to_process.append(spec)
            refs.keys[spec.name] = db.existing_keys(conn, spec)
            ctx.watermarks[spec.name] = db.fetch_watermark(conn, spec.name)
        refs.account_currency = db.existing_account_currency(conn)
    if not to_process:
        return

    known_keys = {name: set(keys) for name, keys in refs.keys.items()}  # curated keys before this batch

    # 3. Schema check + row-level validation, parents first (cascading FK rejection)
    frames: dict[str, pd.DataFrame] = {}
    for spec in to_process:
        frames[spec.name], _extra = read_source(ctx.files[spec.name].raw_path, spec)
    for spec in to_process:
        result = validate_entity(frames[spec.name], spec, refs, now_dt, mask_pii=s.mask_pii)
        ctx.validations[spec.name] = result
        refs.keys[spec.name] |= set(result.valid[spec.pk])
        if spec.name == "accounts":
            refs.account_currency.update(dict(zip(result.valid["account_id"], result.valid["currency"], strict=True)))
        out = ctx.outcomes[spec.name]
        out.source_rows, out.rejected_rows = result.source_rows, result.rejected_rows
        pct = 100.0 * result.rejected_rows / result.source_rows if result.source_rows else 0.0
        log.info("validated", extra={"entity": spec.name, "source_rows": result.source_rows,
                                     "rejected": result.rejected_rows, "reject_pct": round(pct, 2)})
        if pct > s.dq_max_reject_pct:
            raise DataQualityError(
                f"{spec.name}: {pct:.1f}% rows rejected exceeds limit {s.dq_max_reject_pct}%")

    # 4. Incremental filter + hash
    for spec in to_process:
        valid = ctx.validations[spec.name].valid
        cutoff = None if full_refresh else watermark_cutoff(ctx.watermarks[spec.name], s.lookback_hours)
        kept, skipped = apply_watermark(valid, cutoff, spec.pk, known_keys[spec.name])
        ctx.outcomes[spec.name].skipped_watermark = skipped
        ctx.outcomes[spec.name].staged_rows = len(kept)
        ctx.staged[spec.name] = add_row_hash(kept, spec)

    # 5. One atomic transaction (retried only for transient connection errors)
    attempts = max(1, s.db_retry_attempts)

    @retry(attempts=attempts, base_delay=2.0, exceptions=db.TRANSIENT_ERRORS, logger=log)
    def _transaction() -> None:
        _load_transaction(ctx, database, to_process)

    try:
        _transaction()
    except _DryRunRollback:
        log.info("dry run complete; transaction rolled back")


def _load_transaction(ctx: RunContext, database, to_process: list[Entity]) -> None:
    now = fmt_ts(utc_now())
    ctx.recon = []
    with db.begin(database) as conn:
        counts = {}
        for spec in to_process:  # stage + merge parents first so FKs hold
            df = ctx.staged[spec.name]
            cols = [*spec.column_names, "row_hash", "_row_number"]
            records = [{**{k: v for k, v in r.items() if k != "_row_number"},
                        "source_row_number": r["_row_number"]} for r in to_records(df, cols)]
            file_name = ctx.files[spec.name].raw_path.name
            db.stage_rows(conn, spec, records, ctx.load_id, file_name, now)
            counts[spec.name] = db.merge_counts(conn, spec, ctx.load_id)
            db.merge_to_curated(conn, spec, ctx.load_id, now)
            c, o = counts[spec.name], ctx.outcomes[spec.name]
            o.inserted, o.updated, o.unchanged = c.inserted, c.updated, c.unchanged
            log.info("merged", extra={"entity": spec.name, "inserted": c.inserted, "updated": c.updated,
                                      "unchanged": c.unchanged})

        for spec in to_process:
            o = ctx.outcomes[spec.name]
            ctx.recon += reconcile_entity(conn, spec, ctx.load_id, o.source_rows, o.rejected_rows,
                                          o.skipped_watermark, ctx.staged[spec.name], counts[spec.name])
        ctx.recon += referential_checks(conn)

        _persist_diagnostics(conn, ctx, to_process, now)
        failed = [c for c in ctx.recon if c.status == "FAIL"]
        if failed:
            names = ", ".join(f"{c.entity}:{c.check_name}" for c in failed[:5])
            raise ReconciliationError(f"{len(failed)} reconciliation check(s) failed: {names}")

        for spec in to_process:
            f = ctx.files[spec.name]
            db.register_file(conn, spec.name, f.raw_path.name, f.sha256, f.size_bytes,
                             ctx.outcomes[spec.name].source_rows, ctx.load_id, now)
            db.advance_watermark(conn, spec.name, max_watermark(ctx.validations[spec.name].valid),
                                 ctx.load_id, now)
        db.purge_staging(conn, ctx.settings.staging_retention_loads)
        if ctx.dry_run:
            raise _DryRunRollback


def _persist_diagnostics(conn, ctx: RunContext, to_process: list[Entity], now: str) -> None:
    rejects = [r for spec in to_process for r in ctx.validations[spec.name].rejected.to_dict("records")]
    files = {spec.name: ctx.files[spec.name].raw_path.name for spec in to_process}
    stats = [vars(st) for spec in to_process for st in ctx.validations[spec.name].rule_stats]
    db.persist_rejects(conn, ctx.load_id, files, rejects, now)
    db.persist_dq(conn, ctx.load_id, stats, now)
    db.persist_recon(conn, ctx.load_id, [c.as_record() for c in ctx.recon], now)


def _best_effort_diagnostics(ctx: RunContext, database) -> None:
    """After a rollback, still try to keep rejects/DQ/recon evidence for the failed load."""
    if not ctx.validations:
        return
    try:
        specs = [sp for sp in ENTITIES if sp.name in ctx.validations]
        with db.begin(database) as conn:
            _persist_diagnostics(conn, ctx, specs, fmt_ts(utc_now()))
    except Exception:
        log.warning("could not persist failure diagnostics", exc_info=True)
