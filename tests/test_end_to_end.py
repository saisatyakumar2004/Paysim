"""End-to-end: generate -> pipeline (with dbt) -> score against ground truth -> failure scenarios."""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
import pytest

from payreconcile import config, db, ingest, pipeline, reconcile, transform
from payreconcile.evaluate import evaluate_all
from payreconcile.generate import GenConfig, generate

pytestmark = pytest.mark.slow
DAYS = 10
START = date(2026, 1, 1)
LAST = START + timedelta(days=DAYS - 1)


@pytest.fixture(scope="module")
def platform(tmp_path_factory):
    """One shared 10-day run; the scenario tests below each work on top of it, in order."""
    import os
    root = tmp_path_factory.mktemp("payrecon")
    os.environ["PAYRECON_DATA"] = str(root)
    config.settings.cache_clear()
    generate(GenConfig(out=root, days=DAYS, orders_per_day=250, n_merchants=40, seed=7))
    runs = [pipeline.run_day(START + timedelta(days=i)) for i in range(DAYS)]
    yield {"root": root, "runs": runs}
    os.environ.pop("PAYRECON_DATA", None)
    config.settings.cache_clear()


def snapshot(root) -> dict:
    con = duckdb.connect(str(root / "warehouse.duckdb"), read_only=True)
    try:
        return {
            "silver": {t: con.execute(f"SELECT count(*) FROM silver.{t}").fetchone()[0]
                       for t in ("gateway_txns", "settlements", "orders", "merchants_scd2")},
            "recon": con.execute("SELECT status, count(*) FROM gold.fact_reconciliation GROUP BY 1 ORDER BY 1").fetchall(),
            "money": con.execute("SELECT round(sum(money_at_risk), 2) FROM gold.fact_reconciliation").fetchone()[0],
        }
    finally:
        con.close()


def test_every_daily_run_succeeds_and_all_dbt_tests_pass(platform):
    for r in platform["runs"]:
        assert r.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS"), (r.run_date, r.status, r.error, r.reasons)
        assert r.dbt is not None and r.dbt.failed == 0 and r.dbt.passed > 30


def test_quality_layer_catches_injected_defects(platform):
    report = evaluate_all()
    assert report["dq_quarantine"]["recall"] >= 0.99
    assert report["dq_quarantine"]["precision"] >= 0.97


def test_reconciliation_agrees_with_ground_truth(platform):
    report = evaluate_all()
    assert report["recon_status_accuracy"] >= 0.97
    assert report["orphan_settlements"]["recall"] >= 0.95
    assert report["fuzzy_match"]["fp"] == 0                    # ambiguity must never produce a wrong link


def test_incremental_reconciliation_equals_a_full_rebuild_and_the_dbt_reference(platform):
    """Ten days of incremental updates must land on exactly the state a from-scratch rebuild produces, computed two
    independent ways: the engine's own full mode and the separate dbt reference model."""
    con = db.connect()
    assert reconcile.verify(con) == {**reconcile.verify(con), "ok": True, "differ_in_state": 0, "differ_in_full_rebuild": 0}
    con.close()
    res = transform.build(LAST, reference=True)
    assert res.success, res.failures
    con = db.connect()
    cmp = reconcile.compare_with_reference(con)
    con.close()
    assert cmp["ok"], cmp
    assert cmp["state_items"] > 1000


def test_incremental_runs_evaluate_only_part_of_the_state(platform):
    con = duckdb.connect(str(platform["root"] / "warehouse.duckdb"), read_only=True)
    rows = con.execute("SELECT mode, items_evaluated, items_total FROM ops.recon_runs ORDER BY started_at").fetchall()
    con.close()
    assert rows[0][0] == "full" and all(r[0] == "incremental" for r in rows[1:])
    late = rows[-1]
    assert late[1] < 0.6 * late[2], f"day {DAYS} evaluated {late[1]} of {late[2]} items"


def test_audit_trail_explains_the_final_state(platform):
    con = duckdb.connect(str(platform["root"] / "warehouse.duckdb"), read_only=True)
    last = dict(con.execute("SELECT item_id, arg_max(new_status, audit_id) FROM gold.reconciliation_audit "
                            "GROUP BY item_id").fetchall())
    state = dict(con.execute("SELECT item_id, status FROM gold.fact_reconciliation").fetchall())
    n_changes = con.execute("SELECT count(*) FROM gold.reconciliation_audit WHERE change_kind = 'STATUS_CHANGE'").fetchone()[0]
    con.close()
    assert {k: v for k, v in last.items() if v != "CLOSED"} == state
    assert n_changes > 0                       # statuses genuinely moved (pending -> matched/missing, orphans resolved)


def test_no_quarantined_transaction_reaches_gold(platform):
    con = duckdb.connect(str(platform["root"] / "warehouse.duckdb"), read_only=True)
    leaked = con.execute("""SELECT count(*) FROM quarantine.rows q JOIN gold.fact_transactions f
                            ON f.txn_id = q.natural_key
                            WHERE q.dataset = 'gateway' AND q.rule_ids <> 'GW-UNQ-01'""").fetchone()[0]
    con.close()
    assert leaked == 0


def test_rerunning_a_day_is_idempotent(platform):
    before = snapshot(platform["root"])
    r = pipeline.run_day(LAST)
    assert r.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS")
    assert snapshot(platform["root"]) == before


def test_replaying_an_old_day_does_not_rewind_the_reconciliation_clock(platform):
    before = snapshot(platform["root"])
    r = pipeline.run_day(START + timedelta(days=2))
    assert r.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS")
    assert snapshot(platform["root"]) == before
    con = duckdb.connect(str(platform["root"] / "warehouse.duckdb"), read_only=True)
    assert con.execute("SELECT as_of_date FROM ops.recon_state WHERE id = 1").fetchone()[0] == LAST
    con.close()


def test_bad_schema_halts_the_gate_and_leaves_silver_and_gold_untouched(platform):
    path = config.source_dir() / "settlements" / f"settlements_{LAST.isoformat()}.csv"
    original = path.read_text(encoding="utf-8")
    lines = original.splitlines()
    header = lines[0].split(",")
    drop = header.index("gross_amount")
    path.write_text("\n".join(",".join(v for i, v in enumerate(line.split(",")) if i != drop) for line in lines),
                    encoding="utf-8")
    before = snapshot(platform["root"])
    try:
        r = pipeline.run_day(LAST)
        assert r.status == "QUALITY_GATE_FAILED" and r.gate == "HALT"
        assert any("ST-SCH-01" in reason for reason in r.reasons)
        assert snapshot(platform["root"]) == before
    finally:
        path.write_text(original, encoding="utf-8")


def test_api_outage_fails_the_run_cleanly_without_touching_data(platform):
    before = snapshot(platform["root"])
    client = ingest.GatewayClient(fail_first=999, sleep=lambda s: None)
    r = pipeline.run_day(LAST, gateway_client=client)
    assert r.status == "FAILED" and "gateway API failed" in (r.error or "")
    assert snapshot(platform["root"]) == before
    con = duckdb.connect(str(platform["root"] / "warehouse.duckdb"), read_only=True)
    last = con.execute("SELECT status, error FROM ops.run_log ORDER BY started_at DESC LIMIT 1").fetchone()
    con.close()
    assert last[0] == "FAILED"


def test_the_pipeline_recovers_on_the_next_run_after_an_outage(platform):
    r = pipeline.run_day(LAST)
    assert r.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS")


def test_dashboard_renders_against_the_real_warehouse(platform):
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(config.ROOT / "dashboard" / "app.py"), default_timeout=120).run()
    assert not at.exception, [str(e.value)[:300] for e in at.exception]
    assert not at.error
    assert at.title[0].value == "Executive overview"                       # the default page of the 8-page navigation
    assert any(m.label == "Money at risk" for m in at.metric)
    assert any(m.label == "Match rate" for m in at.metric)
