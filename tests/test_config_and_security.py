import re
from pathlib import Path

import pytest

from finflow.config import Settings
from finflow.errors import ConfigError

ROOT = Path(__file__).resolve().parents[1]


def test_mysql_requires_password(monkeypatch, settings):
    monkeypatch.setenv("FINFLOW_DB_BACKEND", "mysql")
    monkeypatch.delenv("MYSQL_PASSWORD", raising=False)
    s = Settings.from_env(load_dotenv_file=False)
    with pytest.raises(ConfigError):
        s.mysql_password()


def test_mysql_password_read_from_env_and_masked_in_logs(monkeypatch):
    monkeypatch.setenv("FINFLOW_DB_BACKEND", "mysql")
    monkeypatch.setenv("MYSQL_PASSWORD", "p@ss:word/1")
    s = Settings.from_env(load_dotenv_file=False)
    assert s.mysql_password() == "p@ss:word/1"
    assert "p@ss" not in s.safe_database_url() and "***" in s.safe_database_url()


def test_invalid_backend_rejected(monkeypatch):
    monkeypatch.setenv("FINFLOW_DB_BACKEND", "oracle")
    with pytest.raises(ConfigError):
        Settings.from_env(load_dotenv_file=False)


def test_no_hardcoded_secrets_in_repo():
    pattern = re.compile(r"""(?i)(password|secret|accountkey|token)\s*=\s*['"][^'"<$\s]{4,}['"]""")
    offenders = []
    for path in list((ROOT / "src").rglob("*.py")):
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.search(line):
                offenders.append(f"{path.relative_to(ROOT)}:{n}")
    assert not offenders, offenders


def test_gitignore_protects_env_and_artifacts():
    text = (ROOT / ".gitignore").read_text()
    for needle in (".env", "*.db", ".venv/", "__pycache__/"):
        assert needle in text
    assert "!.env.example" in text and (ROOT / ".env.example").exists()
    assert not re.search(r"(?m)^[A-Z_]*PASSWORD=\S+", (ROOT / ".env.example").read_text())
