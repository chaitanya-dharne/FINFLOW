"""Runs only when a real MySQL 8.0 is available. Set FINFLOW_TEST_MYSQL_URL, e.g.
mysql://finflow_deploy:<pw>@localhost:3306/finflow_test  (an EMPTY scratch database!)."""
import os
from urllib.parse import unquote, urlparse

import pytest

URL = os.environ.get("FINFLOW_TEST_MYSQL_URL")


@pytest.mark.skipif(not URL, reason="FINFLOW_TEST_MYSQL_URL not set (MySQL integration not exercised)")
def test_pipeline_on_mysql(settings, batch1, batch2):
    import mysql.connector

    from finflow import db
    from finflow.pipeline import run_pipeline

    u = urlparse(URL)

    def opener():
        return mysql.connector.connect(host=u.hostname, port=u.port or 3306, user=unquote(u.username or ""),
                                       password=unquote(u.password or ""), database=u.path.lstrip("/"))

    database = db.Database("mysql", opener, "mysql-test")
    db.init_schema(database, settings.sql_dir)
    r1 = run_pipeline(settings, batch1, database=database)
    r2 = run_pipeline(settings, batch2, database=database)
    assert r1.status == "SUCCESS" and r2.status == "SUCCESS"
    assert all(c.status == "PASS" for c in r1.recon + r2.recon)
    with db.connect(database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM transactions").scalar() > 0
