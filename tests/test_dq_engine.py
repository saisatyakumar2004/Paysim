"""Data-quality engine: every rule is proven with a batch that must fail and a batch that must pass."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from conftest import gw_df, gw_row

from payreconcile import dq

RUN_DATE = date(2026, 1, 2)


def run_gateway(con, *rows):
    return dq.evaluate(con, dq.load_contract("gateway"), gw_df(*rows), RUN_DATE)


def rule(result, rule_id):
    return next(r for r in result.rule_results if r.rule_id == rule_id)


def test_clean_batch_passes_everything(con):
    res = run_gateway(con, gw_row(txn_id="T1"), gw_row(txn_id="T2", order_id="ORD2"))
    assert res.status == "PASS"
    assert res.rows_quarantined == 0 and len(res.good) == 2
    assert res.score == pytest.approx(1.0)


def test_duplicate_transaction_keeps_first_and_quarantines_copy(con):
    res = run_gateway(con, gw_row(), gw_row())
    assert len(res.good) == 1
    assert list(res.quarantined["rule_ids"]) == ["GW-UNQ-01"]
    assert rule(res, "GW-UNQ-01").violations == 1


def test_null_primary_key_is_quarantined(con):
    res = run_gateway(con, gw_row(txn_id=None), gw_row(txn_id="T2"))
    assert len(res.good) == 1
    assert "GW-CMP-01" in res.quarantined["rule_ids"].iloc[0]


@pytest.mark.parametrize("field,value,expected_rule", [
    ("amount", "-50", "GW-VAL-03"),
    ("amount", "N/A", "GW-VAL-02"),
    ("status", "captred", "GW-VAL-04"),
    ("created_at", "2026-02-30T10:00:00", "GW-VAL-01"),          # impossible calendar date
    ("created_at", "2030-01-02T10:00:00", "GW-VAL-01"),          # after the batch date
    ("merchant_id", "M9999", "GW-REF-01"),                       # referential integrity
    ("order_id", None, "GW-CMP-02"),
    ("method", "cheque", "GW-VAL-05"),
])
def test_invalid_values_are_quarantined_with_the_right_rule(con, field, value, expected_rule):
    res = run_gateway(con, gw_row(txn_id="GOOD", order_id="O1"), gw_row(txn_id="BAD", order_id="O2", **{field: value})
                      if field != "order_id" else gw_row(txn_id="BAD", order_id=None))
    assert list(res.good["txn_id"]) == ["GOOD"]
    assert expected_rule in res.quarantined["rule_ids"].iloc[0]


def test_repairable_defects_are_flagged_not_quarantined(con):
    res = run_gateway(con, gw_row(amount="₹1,299.00"), gw_row(txn_id="T2", created_at="02/01/2026 10:00:00"))
    assert res.rows_quarantined == 0 and len(res.good) == 2
    assert rule(res, "GW-FIX-01").violations == 1
    assert rule(res, "GW-FIX-02").violations == 1


def test_missing_required_column_rejects_the_whole_batch(con):
    df = gw_df(gw_row(), gw_row(txn_id="T2")).drop(columns=["amount"])
    res = dq.evaluate(con, dq.load_contract("gateway"), df, RUN_DATE)
    assert res.schema_failed and res.status == "FAIL"
    assert res.good.empty and len(res.quarantined) == 2
    assert set(res.quarantined["rule_ids"]) == {"SCHEMA_REJECT"}


def test_unexpected_column_is_a_warning_and_is_dropped(con):
    df = gw_df(gw_row())
    df["risk_score"] = "0.3"
    res = dq.evaluate(con, dq.load_contract("gateway"), df, RUN_DATE)
    assert res.status == "WARNING" and "risk_score" not in res.good.columns
    assert res.unexpected == ["risk_score"]


def test_renamed_column_is_resolved_through_the_alias(con):
    df = pd.DataFrame([{"settlement_id": "S1", "txn_reference": "GW-TXN1", "merchant_id": "M1", "gross_amount": "100",
                        "fee_amount": "2", "net_amount": "98", "settlement_date": "2026-01-02"}]).astype(object)
    res = dq.evaluate(con, dq.load_contract("settlements"), df, RUN_DATE)
    assert not res.schema_failed and res.aliased == {"txn_reference": "gateway_ref"}
    assert "gateway_ref" in res.good.columns and len(res.good) == 1


def test_business_rule_flags_net_amount_that_does_not_add_up(con):
    df = pd.DataFrame([{"settlement_id": "S1", "gateway_ref": "GW-T1", "merchant_id": "M1", "gross_amount": "100",
                        "fee_amount": "2", "net_amount": "90", "settlement_date": "2026-01-02"}]).astype(object)
    res = dq.evaluate(con, dq.load_contract("settlements"), df, RUN_DATE)
    assert rule(res, "ST-BUS-01").violations == 1 and len(res.good) == 1     # flagged, kept


def test_too_many_bad_rows_turns_the_dataset_into_a_fail_and_halts_the_gate(con):
    rows = [gw_row(txn_id=f"T{i}", order_id=f"O{i}", merchant_id=None if i < 3 else "M1") for i in range(10)]
    res = run_gateway(con, *rows)
    assert rule(res, "GW-CMP-03").status == "FAIL"            # 30% > 10% fail threshold
    gate, _, reasons = dq.gate_decision([res])
    assert gate == "HALT" and any("GW-CMP-03" in r for r in reasons)


def test_score_drops_as_quality_drops(con):
    clean = run_gateway(con, *[gw_row(txn_id=f"T{i}", order_id=f"O{i}") for i in range(20)])
    dirty = run_gateway(con, *[gw_row(txn_id=f"T{i}", order_id=f"O{i}", amount="-1" if i < 4 else "5") for i in range(20)])
    assert clean.score > dirty.score
    assert dq.overall_score([clean]) == pytest.approx(clean.score)


def test_gate_pass_warn_halt_levels(con):
    clean = run_gateway(con, gw_row())
    assert dq.gate_decision([clean])[0] == "PASS"
    warned = run_gateway(con, gw_row(txn_id="A"), gw_row(txn_id="A"), *[gw_row(txn_id=f"T{i}", order_id=f"O{i}") for i in range(200)])
    assert dq.gate_decision([warned])[0] == "WARN"


def test_volume_freshness_and_distribution_checks():
    assert dq.volume_check("gateway", 3000, [3000, 3100, 2900]).status == "PASS"
    assert dq.volume_check("gateway", 500, [3000, 3100, 2900]).status == "FAIL"        # -83%
    assert dq.volume_check("gateway", 1500, [3000, 3100, 2900]).status == "WARNING"    # -50%
    assert dq.volume_check("gateway", 10, [3000]).status == "PASS"                     # not enough history
    assert dq.freshness_check("gateway", pd.Timestamp("2026-01-02 23:00"), RUN_DATE).status == "PASS"
    assert dq.freshness_check("gateway", pd.Timestamp("2025-12-25"), RUN_DATE).status == "FAIL"
    assert dq.freshness_check("gateway", None, RUN_DATE).status == "FAIL"
    hist = [1100.0, 1120.0, 1090.0]
    assert dq.distribution_check("gateway", "mean", 1105.0, hist).status == "PASS"
    assert dq.distribution_check("gateway", "mean", 110500.0, hist).status == "FAIL"   # rupees -> paise
    assert dq.distribution_check("gateway", "mean", 1800.0, hist).status == "WARNING"
