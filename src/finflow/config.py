"""Environment-based configuration. No credentials are ever hardcoded here."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .errors import ConfigError

try:  # python-dotenv is optional at import time
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

PROJECT_ROOT = Path(os.environ.get("FINFLOW_HOME", Path(__file__).resolve().parents[2]))


def _bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _num(name: str, default: float, cast=float):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be numeric, got {raw!r}") from exc


def get_secret(name: str, required: bool = False) -> str | None:
    """Read a secret from the environment (.env file or real environment variable)."""
    value = os.environ.get(name)
    if value:
        return value
    if required:
        raise ConfigError(f"Missing required secret {name} (set it in .env)")
    return None


@dataclass(frozen=True)
class Settings:
    env: str
    project_root: Path
    data_dir: Path
    raw_dir: Path
    reports_dir: Path
    logs_dir: Path
    sql_dir: Path
    db_backend: str
    sqlite_path: Path
    mysql_host: str
    mysql_port: int
    mysql_database: str
    mysql_user: str
    auto_init_schema: bool
    dq_max_reject_pct: float
    lookback_hours: int
    staging_retention_loads: int
    mask_pii: bool
    log_level: str
    db_retry_attempts: int

    @classmethod
    def from_env(cls, load_dotenv_file: bool = True) -> Settings:
        if load_dotenv_file and load_dotenv is not None:
            env_file = PROJECT_ROOT / ".env"
            if env_file.exists():
                load_dotenv(env_file, override=False)
        data_dir = Path(os.environ.get("FINFLOW_DATA_DIR", PROJECT_ROOT / "data"))
        sqlite_path = Path(os.environ.get("FINFLOW_SQLITE_PATH", data_dir / "local" / "finflow.db"))
        if not sqlite_path.is_absolute():
            sqlite_path = PROJECT_ROOT / sqlite_path
        backend = os.environ.get("FINFLOW_DB_BACKEND", "sqlite").strip().lower()
        if backend not in {"sqlite", "mysql"}:
            raise ConfigError("FINFLOW_DB_BACKEND must be 'sqlite' or 'mysql'")
        return cls(
            env=os.environ.get("FINFLOW_ENV", "dev"),
            project_root=PROJECT_ROOT,
            data_dir=data_dir,
            raw_dir=data_dir / "raw",
            reports_dir=data_dir / "reports",
            logs_dir=Path(os.environ.get("FINFLOW_LOGS_DIR", PROJECT_ROOT / "logs")),
            sql_dir=PROJECT_ROOT / "sql",
            db_backend=backend,
            sqlite_path=sqlite_path,
            mysql_host=os.environ.get("MYSQL_HOST", "localhost"),
            mysql_port=int(_num("MYSQL_PORT", 3306, int)),
            mysql_database=os.environ.get("MYSQL_DATABASE", "finflow"),
            mysql_user=os.environ.get("MYSQL_USER", "finflow_etl"),
            auto_init_schema=_bool("FINFLOW_AUTO_INIT_SCHEMA", True),
            dq_max_reject_pct=_num("FINFLOW_DQ_MAX_REJECT_PCT", 20.0),
            lookback_hours=int(_num("FINFLOW_INCREMENTAL_LOOKBACK_HOURS", 24, int)),
            staging_retention_loads=int(_num("FINFLOW_STAGING_RETENTION_LOADS", 5, int)),
            mask_pii=_bool("FINFLOW_MASK_PII_IN_REPORTS", True),
            log_level=os.environ.get("FINFLOW_LOG_LEVEL", "INFO"),
            db_retry_attempts=int(_num("FINFLOW_DB_RETRY_ATTEMPTS", 3, int)),
        )

    def mysql_password(self) -> str:
        return get_secret("MYSQL_PASSWORD", required=True) or ""

    def safe_database_url(self) -> str:
        """Log-safe description of the target database (never contains the password)."""
        if self.db_backend == "sqlite":
            return f"sqlite:///{self.sqlite_path.as_posix()}"
        return f"mysql://{self.mysql_user}:***@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
