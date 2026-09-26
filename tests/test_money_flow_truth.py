"""End to end: generate -> pipeline -> money flow, scored against the financial truth the generator injected.

The truth (truth.py) is derived from the generator's event log, not from the pipeline's output, so agreement here is evidence
about ingestion, quarantine, SCD2 fee lookup, linking and the evidence checks. Thresholds are the measured values for this
seed, with a little slack; a regression in any stage moves them.
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import duckdb
import pandas as pd
import pytest

from payreconcile import config, db, moneyflow, pipeline, reconcile, replay
from payreconcile.evaluate import evaluate_all
from payreconcile.generate import GenConfig, generate

START = date(2026, 1, 1)
DAYS = 9


def run_all(root, days=DAYS, **gen):
    generate(GenConfig(out=root, days=days, orders_per_day=gen.pop("orders_per_day", 350), n_merchants=30,
                       seed=gen.pop("seed", 5), **gen))
    for i in range(days):
        s = pipeline.run_day(START + timedelta(days=i), run_dbt=False)
        assert s.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS"), (s.run_date, s.status, s.error, s.reasons)


@pytest.fixture(scope="module")
def world(tmp_path_factory, request):
    root = tmp_path_factory.mktemp("mf")
    mp = pytest.MonkeyPatch()
    mp.setenv("PAYRECON_DATA", str(root))
    config.settings.cache_clear()
    run_all(root)
    yield {"root": root, "report": evaluate_all(), "truth": pd.read_parquet(root / "truth" / "money_flow.parquet")}
    mp.undo()
    config.settings.cache_clear()


def snapshot(con=None, exclude="run_id"):
    own = con is None
    con = con or duckdb.connect(str(config.warehouse_path()), read_only=True)
    df = con.execute(f"SELECT * EXCLUDE ({exclude}) FROM gold.fact_money_flow ORDER BY flow_id").df()
    ev = con.execute("SELECT * FROM gold.money_flow_evidence ORDER BY flow_id, evidence_type, record_ref, detail").df()
    if own:
        con.close()
    return df, ev


# ---- the pipeline against the injected truth ---------------------------------------------------------------------------
def test_every_chain_in_the_truth_is_a_chain_in_the_pipeline_and_no_other_chain_exists(world):
    c = world["report"]["money_flow"]["chains"]
    assert c["truth_only"] == 0 and c["pipeline_only"] == 0 and c["matched_ids"] == c["truth"] > 2000


def test_no_injected_discrepancy_is_missed_and_no_clean_chain_is_flagged(world):
    d = world["report"]["money_flow"]["discrepancy_detection"]
    assert d["fn"] == 0 and d["recall"] == 1.0
    assert d["precision"] >= 0.99 and d["fp"] <= 1                       # the one tolerated case is a coincidental candidate


def test_stage_root_cause_and_resolution_agree_with_the_truth(world):
    mf = world["report"]["money_flow"]
    assert mf["classification_accuracy"] >= 0.995 and mf["breakpoint_accuracy"] >= 0.995 and mf["resolution_accuracy"] >= 0.995
    assert mf["root_cause"]["correct"] >= mf["root_cause"]["of_truth_discrepancies"] - 2


def test_every_amount_at_every_stage_agrees_with_the_injected_amounts(world):
    for name, r in world["report"]["money_flow"]["field_agreement"].items():
        assert r["differ"] == 0 and r["compared"] > 2000, name


def test_the_money_at_stake_matches_the_truth_within_one_percent(world):
    m = world["report"]["money_flow"]["money"]
    assert m["truth_exposure"] > 0 and m["difference_pct_of_truth"] < 1.0


def test_each_injected_scenario_is_recognised_as_what_it_is(world):
    expected = {"fee_deviation": "FEE_MISMATCH", "late": "TIMING_DELAY", "dup_settlement": "DUPLICATE_SETTLEMENT",
                "split": "MATCHED"}
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    got = con.execute("SELECT flow_id, classification FROM gold.fact_money_flow").df().set_index("flow_id")["classification"]
    con.close()
    truth = world["truth"].set_index("flow_id")
    for scenario, cls in expected.items():
        ids = truth.index[truth["note"] == scenario]
        assert len(ids) >= 1, f"the seed injected no {scenario}: pick another seed"
        stale = truth.loc[ids, "expected_final_status"] == "INSUFFICIENT_EVIDENCE"      # fee schedule row quarantined
        for fid in ids[~stale.to_numpy()]:
            assert got[fid] == cls, (scenario, fid, got[fid])


def test_an_aggregated_settlement_is_never_matched_or_called_missing(world):
    truth = world["truth"]
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    got = con.execute("SELECT flow_id, classification, resolution, root_cause FROM gold.fact_money_flow").df().set_index("flow_id")
    con.close()
    ids = truth.index[truth["note"].isin(["aggregated_over", "aggregated_missing"])]
    assert len(ids) >= 2
    rows = got.loc[truth.loc[ids, "flow_id"]]
    assert set(rows["classification"]) <= {"INSUFFICIENT_EVIDENCE", "PENDING"} and "MATCHED" not in set(rows["classification"])
    assert "POSSIBLE_AGGREGATED_SETTLEMENT" in set(rows["root_cause"])


def test_the_unexplained_residuals_are_exactly_the_injected_net_errors(world):
    mf = world["report"]["money_flow"]["unexplained"]
    assert mf["classified_unexplained"] == mf["truth_unexplained"] > 0


def test_the_known_limits_are_reported_not_hidden(world):
    """Chains the pipeline cannot get right from the data show up as misclassifications, with their cause."""
    mis = world["report"]["money_flow"]["misclassified"]
    allowed = {("ORPHAN_SETTLEMENT", "INSUFFICIENT_EVIDENCE"), ("MISSING_SETTLEMENT", "INSUFFICIENT_EVIDENCE"),
               ("DUPLICATE_PAYMENT", "MATCHED"), ("INSUFFICIENT_EVIDENCE", "ORDER_WITHOUT_PAYMENT")}
    assert {(m["expected_final_status"], m["classification"]) for m in mis} <= allowed


# ---- conservation and traceability on the whole book ------------------------------------------------------------------------
def test_money_is_conserved_and_never_counted_twice_across_the_whole_book(world):
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    inv = moneyflow.check_invariants(con)
    assert inv["ok"], inv
    w = moneyflow.summary(con)["waterfall"]
    assert w["unexplained"] + w["blocked"] + w["explained"] == w["at_risk"]
    assert w["bank_actual"] == pytest.approx(con.execute("SELECT sum(net_amount) FROM silver.settlements").fetchone()[0], abs=0.01)
    steps = moneyflow.summary(con)["steps_by_resolution"]
    net = con.execute("SELECT sum(bank_net) - sum(expected_net) FROM gold.fact_money_flow").fetchone()[0]
    assert (steps["bank_amount"] + steps["fee"] + steps["bank_net_arithmetic"]).sum() == pytest.approx(net, abs=0.05)
    con.close()


def test_every_open_or_blocked_conclusion_has_underlying_records(world):
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    n = con.execute("""SELECT count(*) FROM gold.fact_money_flow f WHERE f.resolution IN ('OPEN', 'BLOCKED', 'EXPLAINED')
                       AND f.exposure_amount > 0 AND f.flow_id NOT IN (SELECT flow_id FROM gold.money_flow_evidence)""").fetchone()[0]
    con.close()
    assert n == 0


def test_every_output_stays_inside_the_declared_vocabulary(world):
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    got = con.execute("SELECT DISTINCT classification, first_breakpoint, resolution FROM gold.fact_money_flow").df()
    con.close()
    assert set(got["classification"]) <= set(moneyflow.CLASSIFICATIONS)
    assert set(got["first_breakpoint"]) <= set(moneyflow.BREAKPOINTS)
    assert set(got["resolution"]) <= set(moneyflow.SEVERITY)


def test_the_merchant_stage_is_never_populated_because_the_source_has_no_such_amount(world):
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    assert con.execute("SELECT count(*) FROM gold.fact_money_flow WHERE merchant_amount IS NOT NULL").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM gold.fact_money_flow WHERE classification = 'MERCHANT_SETTLEMENT_MISMATCH'").fetchone()[0] == 0
    con.close()


# ---- the two build paths, both reconciliation modes, and replay -----------------------------------------------------------------
def test_the_sql_clean_path_and_the_reference_path_produce_the_same_table(world):
    con = db.connect()
    moneyflow.build(con, "fast", fast=True)
    fast, fast_ev = snapshot(con)
    stats = moneyflow.build(con, "slow", fast=False)
    slow, slow_ev = snapshot(con)
    con.close()
    assert stats["clean_path"] == 0
    pd.testing.assert_frame_equal(fast, slow, check_exact=True)
    pd.testing.assert_frame_equal(fast_ev, slow_ev, check_exact=True)
    assert (fast["resolution"] == "RECONCILED").sum() > 0.5 * len(fast)


def test_incremental_and_full_reconciliation_give_identical_money_flow(world):
    before, before_ev = snapshot()
    con = db.connect()
    assert reconcile.verify(con)["ok"]
    as_of = con.execute("SELECT as_of_date FROM ops.recon_state WHERE id = 1").fetchone()[0]
    reconcile.reconcile(con, as_of, "full-rebuild", full=True)
    moneyflow.build(con, "after-full")
    after, after_ev = snapshot(con)
    con.close()
    pd.testing.assert_frame_equal(before, after, check_exact=True)
    pd.testing.assert_frame_equal(before_ev, after_ev, check_exact=True)


def test_replaying_quarantined_records_does_not_duplicate_or_change_money(world):
    before, before_ev = snapshot()
    total = before["bank_net"].sum()
    for _ in range(2):
        for dataset in ("gateway", "settlements", "orders"):
            replay.replay(dataset, START + timedelta(days=3), run_dbt=False)
    after, after_ev = snapshot()
    pd.testing.assert_frame_equal(before, after, check_exact=True)
    pd.testing.assert_frame_equal(before_ev, after_ev, check_exact=True)
    assert after["flow_id"].is_unique and after["bank_net"].sum() == total


# ---- a batch the quality gate refused to load ---------------------------------------------------------------------------------------
def poison_gateway_day(root, day: date) -> None:
    """The gateway silently switches to paise: every row stays valid, only the distribution check notices."""
    for page in sorted((root / "source" / "gateway" / day.isoformat()).glob("page_*.json")):
        payload = json.loads(page.read_text(encoding="utf-8"))
        for rec in payload["data"]:
            try:
                rec["amount"] = float(str(rec["amount"]).replace("₹", "").replace(",", "")) * 100
            except ValueError:
                pass
        page.write_text(json.dumps(payload), encoding="utf-8")


def test_a_halted_batch_blocks_the_conclusions_that_depend_on_it_instead_of_calling_money_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("PAYRECON_DATA", str(tmp_path))
    config.settings.cache_clear()
    generate(GenConfig(out=tmp_path, days=8, orders_per_day=250, n_merchants=25, seed=9))
    bad_day = START + timedelta(days=4)
    poison_gateway_day(tmp_path, bad_day)
    statuses = {}
    for i in range(8):
        s = pipeline.run_day(START + timedelta(days=i), run_dbt=False)
        statuses[START + timedelta(days=i)] = s.status
    assert statuses[bad_day] == "QUALITY_GATE_FAILED"                              # the batch was refused
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    blocked = con.execute("SELECT flow_id, root_cause, resolution, first_breakpoint FROM gold.fact_money_flow "
                          "WHERE classification = 'DATA_QUALITY_BLOCK'").df()
    assert len(blocked) > 0 and set(blocked["resolution"]) == {"BLOCKED"} and set(blocked["first_breakpoint"]) == {"SOURCE_DATA_QUALITY"}
    assert "GATEWAY_BATCH_NOT_LOADED" in set(blocked["root_cause"])
    # every aged order of the halted day is blocked, none is reported as a paid order with no payment
    day_orders = con.execute("SELECT classification FROM gold.fact_money_flow WHERE flow_id LIKE 'ORD:%' AND event_date = ?",
                             [bad_day]).df()
    assert len(day_orders) > 0 and set(day_orders["classification"]) == {"DATA_QUALITY_BLOCK"}
    inv = moneyflow.check_invariants(con)
    con.close()
    assert inv["ok"], inv
    config.settings.cache_clear()
