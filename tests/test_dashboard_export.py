"""Dashboard pages, exceptions-queue filters, and the Power BI export layer, on a small real pipeline run.

Uses a tiny generated dataset with the pipeline run WITHOUT dbt (the dashboard reads Silver + the reconciliation state), so
this stays fast. dbt-dependent marts are optional in the pages and are exercised by the end-to-end suite.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from payreconcile import anomaly, bi_export, config, pipeline  # noqa: E402
from payreconcile.generate import GenConfig, generate  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DASH = str(ROOT / "dashboard")
START = date(2026, 1, 1)
DAYS = 9
LAST = START + timedelta(days=DAYS - 1)
PAGES = ["money_flow", "executive", "reconciliation", "exceptions", "data_quality", "settlement", "merchant_risk",
         "anomaly_signals", "pipeline_health"]


@pytest.fixture(scope="module")
def platform(tmp_path_factory):
    root = tmp_path_factory.mktemp("dash")
    old = os.environ.get("PAYRECON_DATA")
    os.environ["PAYRECON_DATA"] = str(root)
    config.settings.cache_clear()
    generate(GenConfig(out=root, days=DAYS, orders_per_day=220, n_merchants=30, seed=21))
    for i in range(DAYS):
        s = pipeline.run_day(START + timedelta(days=i), run_dbt=False)
        assert s.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS"), s.error
    assert anomaly.run_anomaly_model()["status"] == "OK"
    yield root
    if old is None:
        os.environ.pop("PAYRECON_DATA", None)
    else:
        os.environ["PAYRECON_DATA"] = old
    config.settings.cache_clear()


def _page_script(module: str, dash: str) -> None:
    import importlib
    import sys
    if dash not in sys.path:
        sys.path.insert(0, dash)
    importlib.import_module(f"views.{module}").render()


@pytest.mark.parametrize("module", PAGES)
def test_every_dashboard_page_renders_without_error(platform, module):
    at = AppTest.from_function(_page_script, args=(module, DASH), default_timeout=120).run()
    assert not at.exception, [str(e.value)[:300] for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    assert len(at.title) == 1


def test_the_entry_point_defines_nine_pages_with_stable_urls_and_money_flow_first(platform):
    src = (ROOT / "dashboard" / "app.py").read_text(encoding="utf-8")
    for path in ("money-flow", "overview", "reconciliation", "exceptions", "data-quality", "settlement", "merchant-risk",
                 "anomalies", "pipeline"):
        assert f'"{path}"' in src
    assert src.index('"money-flow"') < src.index('"overview"')
    assert '("Money Flow", "money-flow", money_flow.render, True)' in src            # the default landing page


def _investigation_script(flow_id: str, dash: str) -> None:
    import sys
    if dash not in sys.path:
        sys.path.insert(0, dash)
    import views.money_flow as mf
    mf._investigation(flow_id)


def test_the_money_flow_page_shows_the_headline_the_waterfall_and_the_conservation_check(platform):
    at = AppTest.from_function(_page_script, args=("money_flow", DASH), default_timeout=120).run()
    assert not at.exception, [str(e.value)[:300] for e in at.exception]
    info = " ".join(i.value for i in at.info)
    assert "was expected from customers" in info and "reached the bank" in info and "not in the source data" in info
    assert not at.error                                                                # no conservation check failed
    labels = {m.label for m in at.metric}
    assert {"Expected settlement", "Reconciled", "Money at risk", "Unexplained", "Blocked / unverifiable", "In flight"} <= labels
    frames = [df.value for df in at.dataframe]
    waterfall = next(f for f in frames if "step" in f.columns and "Merchant received" in set(f["step"]))
    row = waterfall[waterfall["step"] == "Merchant received"].iloc[0]
    assert "NOT AVAILABLE" in row["note"] and pd.isna(row["INR"])                       # never fabricated


@pytest.mark.parametrize("classification", ["MATCHED", "MISSING_SETTLEMENT", "BANK_AMOUNT_MISMATCH", "QUARANTINED_SOURCE"])
def test_the_investigation_view_shows_every_stage_and_the_evidence_for_a_chain(platform, classification):
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    row = con.execute("SELECT flow_id FROM gold.fact_money_flow WHERE classification = ? ORDER BY flow_id LIMIT 1",
                      [classification]).fetchone()
    con.close()
    if row is None:
        pytest.skip(f"no {classification} chain in this small dataset")
    at = AppTest.from_function(_investigation_script, args=(row[0], DASH), default_timeout=120).run()
    assert not at.exception, [str(e.value)[:300] for e in at.exception]
    stage_table = next(df.value for df in at.dataframe if "stage" in df.value.columns and "step" in df.value.columns)
    assert list(stage_table["stage"]) == ["ORDER", "GATEWAY", "FEE", "EXPECTED", "BANK", "MERCHANT", "RESULT"]
    verdict = " ".join(x.value for group in (at.error, at.warning, at.info, at.success) for x in group)
    assert classification in verdict


def test_the_anomaly_page_carries_the_not_a_fraud_detector_warning(platform):
    at = AppTest.from_function(_page_script, args=("anomaly_signals", DASH), default_timeout=120).run()
    warning = " ".join(w.value for w in at.warning)
    assert "not a proven fraud detector" in warning and "synthetic" in warning


def test_every_kpi_has_definition_formula_meaning_and_sources():
    sys.path.insert(0, DASH)
    from kpis import KPIS
    assert len(KPIS) >= 12
    for k in KPIS.values():
        assert k.definition and k.formula and k.meaning and k.sources, k.key


# ---- exceptions queue filters ---------------------------------------------------------------------------------------
def run_filter(**kw):
    sys.path.insert(0, DASH)
    from views.exceptions import FLAGS, build_query
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    asof = con.execute("SELECT as_of_date FROM ops.recon_state WHERE id = 1").fetchone()[0]
    args = dict(statuses=["AMOUNT_MISMATCH", "MISSING_SETTLEMENT", "ORPHAN_REFUND", "ORPHAN_SETTLEMENT",
                          "ORDER_WITHOUT_PAYMENT", "SETTLED_NOT_CAPTURED"], flags=[], merchant="", dates=(START, LAST),
                tiers=[], methods=[], amount_range=(0.0, 1e12), age_range=(0, 10_000), asof=asof)
    args.update(kw)
    sql, params = build_query(**args)
    df = con.execute(sql, params).df()
    con.close()
    return df, FLAGS


def test_filter_by_status_returns_only_that_status(platform):
    df, _ = run_filter(statuses=["MISSING_SETTLEMENT"])
    assert len(df) > 0 and set(df["status"]) == {"MISSING_SETTLEMENT"}


def test_filter_by_merchant_risk_tier_and_payment_method(platform):
    every, _ = run_filter()
    assert len(every) > 0
    tier = every["risk_tier"].dropna().iloc[0]
    df, _ = run_filter(tiers=[tier])
    assert set(df["risk_tier"]) == {tier} and len(df) <= len(every)
    method_rows = every.dropna(subset=["payment_method"])
    method = method_rows["payment_method"].iloc[0]
    df, _ = run_filter(methods=[method])
    assert set(df["payment_method"]) == {method}


def test_filter_by_amount_range_and_age(platform):
    every, _ = run_filter()
    lo, hi = every["amount"].quantile(0.25), every["amount"].quantile(0.75)
    df, _ = run_filter(amount_range=(float(lo), float(hi)))
    assert len(df) > 0 and df["amount"].between(lo, hi).all()
    old, _ = run_filter(age_range=(4, 10_000))
    assert (old["age_days"] >= 4).all()
    young, _ = run_filter(age_range=(0, 3))
    assert (young["age_days"] <= 3).all() and len(old) + len(young) == len(every)


def test_filter_by_date_range_and_merchant_text(platform):
    df, _ = run_filter(dates=(START + timedelta(days=2), START + timedelta(days=4)))
    assert df["event_date"].between(pd.Timestamp(START + timedelta(days=2)), pd.Timestamp(START + timedelta(days=4))).all()
    assert len(df) > 0
    mid = run_filter()[0]["merchant_id"].dropna().iloc[0]
    only, _ = run_filter(merchant=mid)
    assert set(only["merchant_id"]) == {mid}


def test_flags_add_duplicate_charges_and_no_filter_means_no_rows(platform):
    plain, _ = run_filter(statuses=[], flags=[])
    assert plain.empty                                                   # nothing selected -> nothing shown, not everything
    dups, _ = run_filter(statuses=[], flags=["Confirmed duplicate charge"])
    assert len(dups) > 0 and dups["dup_confirmed"].all()


def test_every_exception_row_is_traceable_to_source_records(platform):
    from payreconcile import db
    from payreconcile.explain import explain
    df, _ = run_filter()
    con = db.connect(read_only=True)
    for item_id in list(df["item_id"].head(5)):
        info = explain(con, item_id, include_bronze=False)
        assert info["current"]["item_id"] == item_id
        assert any(info["silver_records"].values()) or info["quarantine_rows"], item_id     # a source record exists
        assert info["audit_history"], item_id                                               # and a status history
    con.close()


# ---- Power BI export ---------------------------------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def exported(platform, tmp_path_factory):
    out = tmp_path_factory.mktemp("bi")
    manifest = bi_export.export(out)
    return out, manifest


def test_export_writes_every_table_in_both_formats(exported):
    out, manifest = exported
    expected = {"dim_date", "dim_merchant", "dim_payment_method", "dim_status", "dim_stage", "dim_resolution",
                "fact_transactions", "fact_settlements", "fact_reconciliation", "fact_reconciliation_audit", "fact_money_flow",
                "money_flow_bridge", "money_flow_evidence", "kpi_daily", "dq_scores", "pipeline_runs"}
    assert set(manifest["tables"]) == expected
    for t in expected:
        assert (out / f"{t}.csv").exists() and (out / f"{t}.parquet").exists()
    assert json.loads((out / "manifest.json").read_text(encoding="utf-8"))["tables"]["fact_reconciliation"]["rows"] > 0


def test_export_row_counts_match_the_warehouse(platform, exported):
    _, manifest = exported
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    for table, src in (("fact_transactions", "silver.gateway_txns"), ("fact_settlements", "silver.settlements"),
                       ("fact_reconciliation", "gold.fact_reconciliation"), ("dim_merchant", "silver.merchants_scd2"),
                       ("fact_reconciliation_audit", "gold.reconciliation_audit"), ("fact_money_flow", "gold.fact_money_flow"),
                       ("money_flow_evidence", "gold.money_flow_evidence")):
        assert manifest["tables"][table]["rows"] == con.execute(f"SELECT count(*) FROM {src}").fetchone()[0], table
    con.close()


def test_export_keys_are_unique_and_relationships_have_no_orphans(exported):
    out, _ = exported
    con = duckdb.connect()

    def q(sql):
        return con.execute(sql.replace("$", out.as_posix() + "/")).fetchone()[0]

    assert q("SELECT count(*) - count(DISTINCT item_id) FROM read_parquet('$fact_reconciliation.parquet')") == 0
    assert q("SELECT count(*) - count(DISTINCT txn_id) FROM read_parquet('$fact_transactions.parquet')") == 0
    assert q("SELECT count(*) - count(DISTINCT merchant_sk) FROM read_parquet('$dim_merchant.parquet')") == 0
    for fact in ("fact_reconciliation", "fact_transactions", "fact_settlements"):
        orphan_merchants = q(f"SELECT count(*) FROM read_parquet('$'||'{fact}.parquet') f WHERE f.merchant_sk IS NOT NULL AND "
                             f"f.merchant_sk NOT IN (SELECT merchant_sk FROM read_parquet('$dim_merchant.parquet'))")
        assert orphan_merchants == 0, fact
        assert q(f"SELECT count(*) FROM read_parquet('$'||'{fact}.parquet') f WHERE f.date_key NOT IN "
                 f"(SELECT date_key FROM read_parquet('$dim_date.parquet'))") == 0, fact
    assert q("SELECT count(*) FROM read_parquet('$fact_reconciliation.parquet') WHERE status NOT IN "
             "(SELECT status FROM read_parquet('$dim_status.parquet'))") == 0
    con.close()


def test_export_kpis_agree_with_the_reconciliation_state_and_the_daily_table(platform, exported):
    out, _ = exported
    con = duckdb.connect()
    p = out.as_posix()
    money_fact = con.execute(f"SELECT sum(money_at_risk) FROM read_parquet('{p}/fact_reconciliation.parquet')").fetchone()[0]
    money_daily = con.execute(f"SELECT sum(money_at_risk) FROM read_parquet('{p}/kpi_daily.parquet')").fetchone()[0]
    src = duckdb.connect(str(config.warehouse_path()), read_only=True)
    money_state = src.execute("SELECT sum(money_at_risk) FROM gold.fact_reconciliation").fetchone()[0]
    exc_state = src.execute("SELECT count(*) FROM gold.fact_reconciliation WHERE status IN ('AMOUNT_MISMATCH', "
                            "'MISSING_SETTLEMENT', 'ORPHAN_REFUND', 'ORPHAN_SETTLEMENT', 'ORDER_WITHOUT_PAYMENT', "
                            "'SETTLED_NOT_CAPTURED')").fetchone()[0]
    src.close()
    exc_export = con.execute(f"SELECT count(*) FROM read_parquet('{p}/fact_reconciliation.parquet') WHERE is_exception").fetchone()[0]
    assert money_fact == pytest.approx(money_state) == pytest.approx(money_daily)
    assert exc_export == exc_state
    con.close()


def test_the_export_carries_the_financial_lineage_and_root_cause_fields_powerbi_needs(exported):
    out, manifest = exported
    cols = set(manifest["tables"]["fact_money_flow"]["columns"])
    assert {"order_amount", "gateway_gross", "expected_fee", "expected_net", "bank_net", "merchant_amount", "gap_gateway",
            "gap_bank_amount", "gap_fee", "gap_bank_net", "difference_amount", "classification", "first_breakpoint",
            "root_cause", "resolution", "evidence_level", "exposure_amount", "counted_in", "settlement_ids",
            "merchant_sk", "date_key"} <= cols
    con = duckdb.connect()
    p = out.as_posix()

    def q(sql):
        return con.execute(sql.replace("$", p + "/")).fetchone()[0]

    assert q("SELECT count(*) - count(DISTINCT flow_id) FROM read_parquet('$fact_money_flow.parquet')") == 0
    assert q("SELECT count(*) FROM read_parquet('$fact_money_flow.parquet') WHERE merchant_sk IS NOT NULL AND merchant_sk NOT IN "
             "(SELECT merchant_sk FROM read_parquet('$dim_merchant.parquet'))") == 0
    assert q("SELECT count(*) FROM read_parquet('$fact_money_flow.parquet') WHERE first_breakpoint NOT IN "
             "(SELECT stage FROM read_parquet('$dim_stage.parquet'))") == 0
    assert q("SELECT count(*) FROM read_parquet('$fact_money_flow.parquet') WHERE resolution NOT IN "
             "(SELECT resolution FROM read_parquet('$dim_resolution.parquet'))") == 0
    assert q("SELECT count(*) FROM read_parquet('$fact_money_flow.parquet') WHERE merchant_amount IS NOT NULL") == 0
    # the bridge is the same money, one row per chain and step: it sums back to the wide table's gaps
    wide = q("SELECT round(sum(gap_gateway + gap_bank_amount + gap_fee + gap_bank_net), 2) FROM read_parquet('$fact_money_flow.parquet')")
    long_ = q("SELECT round(sum(amount), 2) FROM read_parquet('$money_flow_bridge.parquet')")
    assert wide == pytest.approx(long_, abs=0.01)
    assert q("SELECT count(*) FROM read_parquet('$money_flow_bridge.parquet') WHERE flow_id NOT IN "
             "(SELECT flow_id FROM read_parquet('$fact_money_flow.parquet'))") == 0
    assert q("SELECT count(*) FROM read_parquet('$money_flow_evidence.parquet') WHERE flow_id NOT IN "
             "(SELECT flow_id FROM read_parquet('$fact_money_flow.parquet'))") == 0
    # every open or blocked finding can be traced to at least one underlying record
    assert q("SELECT count(*) FROM read_parquet('$fact_money_flow.parquet') f WHERE f.resolution IN ('OPEN', 'BLOCKED') "
             "AND f.exposure_amount > 0 AND f.flow_id NOT IN (SELECT flow_id FROM read_parquet('$money_flow_evidence.parquet'))") == 0
    con.close()


def test_export_refuses_to_run_before_the_pipeline_has_run(tmp_path, monkeypatch):
    monkeypatch.setenv("PAYRECON_DATA", str(tmp_path))
    config.settings.cache_clear()
    with pytest.raises(RuntimeError, match="run the pipeline"):
        bi_export.export(tmp_path / "out")
