"""Partial pipeline failures: every stage that can fail must leave a state that the next run repairs, with no manual cleanup."""

from __future__ import annotations

from datetime import date

import pytest

from payreconcile import db, pipeline, reconcile, transform
from payreconcile.generate import GenConfig, generate

D1, D2, D3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
OK = ("SUCCESS", "SUCCESS_WITH_WARNINGS")


@pytest.fixture()
def small(data_dir):
    generate(GenConfig(out=data_dir, days=4, orders_per_day=120, n_merchants=20, seed=4))
    return data_dir


def state():
    con = db.connect(read_only=True)
    try:
        return {"items": con.execute("SELECT count(*) FROM gold.fact_reconciliation").fetchone()[0],
                "by_status": con.execute("SELECT status, count(*) FROM gold.fact_reconciliation GROUP BY 1 ORDER BY 1").fetchall(),
                "watermark": con.execute("SELECT last_seq FROM ops.recon_state").fetchone()}
    finally:
        con.close()


def test_a_crash_after_silver_is_loaded_but_before_reconciliation_is_repaired_by_the_next_run(small, monkeypatch):
    assert pipeline.run_day(D1, run_dbt=False).status in OK
    before = state()

    def boom(*args, **kwargs):
        raise RuntimeError("simulated crash between Silver and reconciliation")

    monkeypatch.setattr(pipeline.reconcile, "reconcile", boom)
    s = pipeline.run_day(D2, run_dbt=False)
    assert s.status == "FAILED" and "simulated crash" in s.error
    con = db.connect(read_only=True)
    assert con.execute("SELECT count(*) FROM silver.gateway_txns WHERE created_at::DATE = ?", [D2]).fetchone()[0] > 0   # Silver committed
    con.close()
    assert state() == before                                                # reconciliation state untouched, watermark not advanced

    monkeypatch.undo()
    assert pipeline.run_day(D2, run_dbt=False).status in OK                 # simply run the day again
    con = db.connect()
    assert reconcile.verify(con)["ok"]                                      # caught up: identical to a from-scratch recompute
    assert con.execute("SELECT count(*) FROM gold.fact_reconciliation").fetchone()[0] > before["items"]
    con.close()


def test_a_dbt_failure_after_reconciliation_keeps_the_reconciliation_and_recovers_next_run(small, monkeypatch):
    assert pipeline.run_day(D1, run_dbt=False).status in OK
    monkeypatch.setattr(pipeline.transform, "build",
                        lambda *a, **k: transform.DbtResult(False, passed=10, failed=1, failures=["assert_scd2_integrity: fail"]))
    s = pipeline.run_day(D2, run_dbt=True)
    assert s.status == "FAILED_TESTS" and "assert_scd2_integrity" in s.error
    con = db.connect()
    assert reconcile.verify(con)["ok"]                                      # the engine's state is committed and consistent
    con.close()
    monkeypatch.undo()
    assert pipeline.run_day(D3, run_dbt=False).status in OK
    con = db.connect()
    assert reconcile.verify(con)["ok"]
    con.close()


def test_a_failed_run_is_visible_in_the_run_log_with_its_error(small, monkeypatch):
    monkeypatch.setattr(pipeline.silver, "apply_gateway", lambda *a, **k: (_ for _ in ()).throw(ValueError("bad batch")))
    s = pipeline.run_day(D1, run_dbt=False)
    assert s.status == "FAILED"
    con = db.connect(read_only=True)
    row = con.execute("SELECT status, error FROM ops.run_log ORDER BY started_at DESC LIMIT 1").fetchone()
    tasks = con.execute("SELECT task, status FROM ops.task_log WHERE run_id = ? ORDER BY task", [s.run_id]).fetchall()
    con.close()
    assert row[0] == "FAILED" and "bad batch" in row[1]
    assert ("load_silver", "FAILED") in tasks


def test_rerunning_a_day_after_every_kind_of_failure_never_duplicates_rows(small, monkeypatch):
    monkeypatch.setattr(pipeline.reconcile, "reconcile", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    for _ in range(3):                                                      # three failed attempts in a row
        assert pipeline.run_day(D1, run_dbt=False).status == "FAILED"
    monkeypatch.undo()
    assert pipeline.run_day(D1, run_dbt=False).status in OK
    con = db.connect(read_only=True)
    dup = con.execute("SELECT count(*) - count(DISTINCT txn_id) FROM silver.gateway_txns").fetchone()[0]
    dup_items = con.execute("SELECT count(*) - count(DISTINCT item_id) FROM gold.fact_reconciliation").fetchone()[0]
    con.close()
    assert dup == 0 and dup_items == 0
