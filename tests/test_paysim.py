"""PaySim pipeline: schema, data quality, balance consistency per transaction type, duplicates, fraud metrics, features, Gold tables,
sampling, temporal split and target leakage. Hand-built fixtures with hand-computed expectations, plus a small synthetic file for the
sampling and ML tests. Everything is deterministic."""

from __future__ import annotations

import csv
import hashlib
import json
import math

import duckdb
import numpy as np
import pandas as pd
import pytest

from payreconcile import config
from payreconcile.paysim import (
    bronze,
    export,
    features,
    fraud,
    gold,
    ml,
    pipeline,
    profile,
    quality,
    queries,
    sample,
)
from payreconcile.paysim.common import SOURCE_COLUMNS, Dataset, connect

COLS = list(SOURCE_COLUMNS)

# 15 rows, every expected outcome worked out by hand (comments give the arithmetic).
VALID = [
    (1, "PAYMENT", 100.0, "C1", 1000.0, 900.0, "M1", 0.0, 0.0, 0, 0),         # 1  origin 1000-100=900 ok; merchant dest -> NOT_APPLICABLE
    (1, "TRANSFER", 500.0, "C2", 500.0, 0.0, "C3", 1000.0, 1500.0, 1, 0),      # 2  fraud, not flagged (FN); drains origin; all consistent
    (2, "CASH_OUT", 200.0, "C3", 1500.0, 1000.0, "C4", 100.0, 300.0, 0, 0),    # 3  origin expects 1300, has 1000: error -300
    (2, "CASH_IN", 300.0, "C5", 100.0, 400.0, "C6", 1000.0, 700.0, 0, 0),      # 4  CASH_IN: origin +300, destination (agent) -300
    (3, "TRANSFER", 1000.0, "C7", 0.0, 0.0, "C8", 0.0, 0.0, 0, 0),             # 5  no balances recorded at all; debit above balance
    (3, "DEBIT", 50.0, "C9", 200.0, 150.0, "C10", 10.0, 60.0, 0, 0),           # 6  DEBIT credits the destination
    (4, "PAYMENT", 0.0, "C11", 100.0, 100.0, "M2", 0.0, 0.0, 0, 0),           # 7  zero amount
    (1, "PAYMENT", 100.0, "C1", 1000.0, 900.0, "M1", 0.0, 0.0, 0, 0),          # 8  exact duplicate of row 1 (later occurrence)
    (5, "TRANSFER", 300000.0, "C12", 300000.0, 0.0, "C13", 0.0, 0.0, 1, 1),    # 9  fraud AND flagged (TP); destination balances unrecorded
    (5, "TRANSFER", 250000.0, "C14", 900000.0, 650000.0, "C15", 5.0, 250005.0, 0, 1),  # 10 flagged but not fraud (FP)
    (6, "CASH_OUT", 10.0, "C16", 100.0, 90.0, "C16", 0.0, 10.0, 0, 0),         # 11 origin == destination
    (6, "PAYMENT", 33.33, "C17", 100.0, 66.68, "M3", 0.0, 0.0, 0, 0),          # 12 one cent of rounding: within tolerance
    (7, "PAYMENT", 33.33, "C18", 100.0, 66.70, "M4", 0.0, 0.0, 0, 0),          # 13 three cents off: inconsistent
    (7, "CASH_OUT", 50.0, "C19", 0.0, 0.0, "C20", 20.0, 70.0, 0, 0),           # 14 origin unrecorded, destination consistent
    (8, "TRANSFER", 1000.0, "C21", 1000.0, 0.0, "C22", 0.0, 0.0, 1, 0),        # 15 fraud not flagged (FN); destination unrecorded
]
EXPECTED_STATUS = {1: "CONSISTENT", 2: "CONSISTENT", 3: "INCONSISTENT", 4: "CONSISTENT", 5: "INSUFFICIENT_EVIDENCE", 6: "CONSISTENT",
                   7: "CONSISTENT", 8: "CONSISTENT", 9: "INSUFFICIENT_EVIDENCE", 10: "CONSISTENT", 11: "CONSISTENT", 12: "CONSISTENT",
                   13: "INCONSISTENT", 14: "INSUFFICIENT_EVIDENCE", 15: "INSUFFICIENT_EVIDENCE"}

# one row of each kind of invalid record, plus one good row
INVALID = [
    (1, "PAYMENT", 10.0, "C1", 100.0, 90.0, "M1", 0.0, 0.0, 0, 0),             # 1 good
    (1, "PAYMENT", -5.0, "C2", 100.0, 105.0, "M1", 0.0, 0.0, 0, 0),            # 2 negative amount
    (1, "REFUND", 5.0, "C3", 100.0, 95.0, "M1", 0.0, 0.0, 0, 0),               # 3 unknown type
    (1, "PAYMENT", 5.0, "C4", 100.0, 95.0, "M1", 0.0, 0.0, 2, 0),              # 4 invalid fraud flag
    (1, "PAYMENT", 5.0, "C5", -1.0, 4.0, "M1", 0.0, 0.0, 0, 0),                # 5 negative balance
    (1, "PAYMENT", 5.0, "C6", 100.0, 95.0, "", 0.0, 0.0, 0, 0),                # 6 null destination
    (0, "PAYMENT", 5.0, "C7", 100.0, 95.0, "M1", 0.0, 0.0, 0, 0),              # 7 step below 1
    (1, "PAYMENT", "abc", "C8", 100.0, 95.0, "M1", 0.0, 0.0, 0, 0),            # 8 non-numeric amount
]
EXPECTED_REJECTS = {2: "negative_amount", 3: "invalid_transaction_type", 4: "invalid_fraud_flag", 5: "negative_balance",
                    6: "null_or_unparseable_fields", 7: "invalid_step", 8: "null_or_unparseable_fields"}


def write_csv(path, rows, header=None):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header or COLS)
        w.writerows(rows)
    return path


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PAYRECON_DATA", str(tmp_path))
    config.settings.cache_clear()
    yield tmp_path
    config.settings.cache_clear()


def build(data_dir, rows, name="t", header=None, upto="gold"):
    ds = Dataset(name, write_csv(data_dir / f"{name}.csv", rows, header))
    bronze.ingest_bronze(ds)
    if upto in ("quality", "gold"):
        quality.run_quality(ds)
    if upto == "gold":
        gold.build_gold(ds)
    return ds


@pytest.fixture()
def valid(data_dir):
    return build(data_dir, VALID)


def q(ds, sql, params=()):
    con = connect(ds, read_only=True) if ds.db.exists() else duckdb.connect()     # Bronze checks read Parquet before any database exists
    try:
        return con.execute(sql, list(params)).df()
    finally:
        con.close()


def rules(ds):
    return q(ds, "SELECT * FROM silver.dq_results").set_index("rule_name")


# ---- schema and Bronze ---------------------------------------------------------------------------------------------------------------
def test_a_source_missing_required_columns_is_rejected_and_names_them(data_dir):
    rows = [r[:2] + r[3:] for r in VALID[:2]]
    header = [c for c in COLS if c != "amount"]
    ds = Dataset("t", write_csv(data_dir / "t.csv", rows, header))
    with pytest.raises(bronze.SchemaError, match="amount"):
        bronze.ingest_bronze(ds)
    assert bronze.check_schema(COLS)["missing"] == []
    assert bronze.check_schema(COLS + ["extra"])["extra"] == ["extra"]


def test_bronze_keeps_the_source_values_unchanged_and_adds_lineage(data_dir):
    src = write_csv(data_dir / "t.csv", VALID)
    before = hashlib.sha256(src.read_bytes()).hexdigest()
    ds = Dataset("t", src)
    meta = bronze.ingest_bronze(ds)
    assert meta["rows"] == len(VALID) and meta["typed_read"] and meta["schema"]["missing"] == []
    b = q(ds, f"SELECT * FROM read_parquet('{ds.bronze.as_posix()}') ORDER BY source_row_number")
    assert list(b["source_row_number"]) == list(range(1, len(VALID) + 1))               # traceable to the source line
    assert set(b["source_file"]) == {"t.csv"} and b["ingestion_timestamp"].notna().all()
    for i, row in enumerate(VALID):                                                       # no business value was altered
        got = b.iloc[i]
        assert [got[c] for c in COLS] == list(row)
    assert hashlib.sha256(src.read_bytes()).hexdigest() == before                         # the raw input is immutable


def test_values_that_do_not_parse_are_kept_as_text_not_dropped(data_dir):
    ds = build(data_dir, INVALID, upto="bronze")
    meta = json.loads((ds.dir / "bronze.meta.json").read_text(encoding="utf-8"))
    assert meta["typed_read"] is False and meta["rows"] == len(INVALID)
    b = q(ds, f"SELECT amount FROM read_parquet('{ds.bronze.as_posix()}') WHERE source_row_number = 8")
    assert b["amount"].iloc[0] == "abc"


# ---- data-quality rules ----------------------------------------------------------------------------------------------------------------
def test_every_rule_reports_name_checked_failed_pass_rate_and_severity(valid):
    r = q(valid, "SELECT * FROM silver.dq_results")
    assert {"rule_name", "total_checked", "failed_count", "pass_rate", "severity"} <= set(r.columns)
    assert set(r["severity"]) == {"ERROR", "WARNING", "INFO"}
    for _, x in r.iterrows():
        if x["total_checked"]:
            assert x["pass_rate"] == pytest.approx(1 - x["failed_count"] / x["total_checked"])
    for name in ("required_columns", "null_or_unparseable_fields", "negative_amount", "zero_amount", "invalid_transaction_type",
                 "invalid_fraud_flag", "negative_balance", "duplicate_records", "origin_balance_inconsistent",
                 "destination_balance_inconsistent", "origin_overdraw"):
        assert name in set(r["rule_name"]), name


def test_rule_counts_on_the_hand_built_file(valid):
    r = rules(valid)
    checks = {"zero_amount": (15, 1), "duplicate_records": (15, 1), "origin_equals_destination": (15, 1), "origin_overdraw": (13, 2),
              "flag_outside_transfer": (2, 0), "origin_balance_inconsistent": (13, 2), "destination_balance_inconsistent": (7, 0),
              "origin_balance_evidence_missing": (15, 2), "destination_balance_evidence_missing": (10, 3), "destination_is_merchant": (15, 5),
              "required_columns": (11, 0)}
    for name, (total, failed) in checks.items():
        assert (int(r.loc[name, "total_checked"]), int(r.loc[name, "failed_count"])) == (total, failed), name
    assert int(r.loc[[n for n in r.index if r.loc[n, "severity"] == "ERROR"], "failed_count"].sum()) == 0


def test_only_the_later_copy_of_an_exact_duplicate_is_flagged_and_a_near_duplicate_is_not(data_dir):
    rows = [VALID[0], VALID[0], (1, "PAYMENT", 100.0, "C1", 1000.0, 900.0, "M1", 0.0, 0.0, 1, 0)]      # 2nd identical, 3rd differs in isFraud
    ds = build(data_dir, rows)
    t = q(ds, "SELECT source_row_number, dq_warning_rules FROM silver.transactions ORDER BY 1")
    assert list(t["source_row_number"]) == [1, 2, 3]                                       # duplicates are flagged, never removed
    assert [w if isinstance(w, str) else "" for w in t["dq_warning_rules"]] == ["", "duplicate_record", ""]


def test_rows_that_fail_an_error_rule_are_quarantined_with_their_reason_and_nothing_is_lost(data_dir):
    ds = build(data_dir, INVALID)
    res = quality.run_quality(ds)
    assert res["silver_rows"] == 1 and res["quarantined_rows"] == 7 and res["conserved"]
    qr = q(ds, "SELECT source_row_number, dq_error_rules FROM silver.dq_quarantine ORDER BY 1")
    assert dict(zip(qr["source_row_number"], qr["dq_error_rules"], strict=True)) == EXPECTED_REJECTS
    assert list(q(ds, "SELECT source_row_number FROM silver.transactions")["source_row_number"]) == [1]
    r = rules(ds)
    for rule in ("negative_amount", "invalid_transaction_type", "invalid_fraud_flag", "negative_balance", "invalid_step"):
        assert int(r.loc[rule, "failed_count"]) == 1, rule
    assert int(r.loc["null_or_unparseable_fields", "failed_count"]) == 2
    assert (r.loc["null_or_unparseable_fields", "pass_rate"]) == pytest.approx(1 - 2 / 8)


def test_zero_amounts_and_zero_balances_are_warnings_or_evidence_limits_not_errors(valid):
    t = q(valid, "SELECT * FROM silver.transactions WHERE source_row_number IN (5, 7) ORDER BY 1")
    assert len(t) == 2                                                                   # both are kept
    five, seven = t.iloc[0], t.iloc[1]
    assert five["balance_consistency_status"] == "INSUFFICIENT_EVIDENCE" and five["origin_balance_status"] == "INSUFFICIENT_EVIDENCE"
    assert pd.isna(five["origin_balance_error"]) and pd.isna(five["origin_balance_consistent"])   # unavailable evidence is not "consistent"
    assert "origin_overdraw" in five["dq_warning_rules"]
    assert seven["dq_status"] == "WARNING" and "zero_amount" in seven["dq_warning_rules"]


# ---- balance semantics per type ------------------------------------------------------------------------------------------------------------
def test_each_row_gets_the_status_its_transaction_type_implies(valid):
    t = q(valid, "SELECT source_row_number, balance_consistency_status FROM silver.transactions ORDER BY 1")
    assert dict(zip(t["source_row_number"], t["balance_consistency_status"], strict=True)) == EXPECTED_STATUS


def test_balance_errors_use_the_right_equation_for_each_type(valid):
    t = q(valid, "SELECT * FROM silver.transactions").set_index("source_row_number")
    assert t.loc[3, "expected_new_balance_org"] == 1300 and t.loc[3, "origin_balance_error"] == pytest.approx(-300)   # CASH_OUT: old - amount
    assert t.loc[4, "expected_new_balance_org"] == 400 and t.loc[4, "origin_balance_error"] == pytest.approx(0)       # CASH_IN: old + amount
    assert t.loc[4, "expected_new_balance_dest"] == 700 and t.loc[4, "destination_balance_error"] == pytest.approx(0)  # CASH_IN dest: old - amount
    assert t.loc[6, "expected_new_balance_dest"] == 60 and t.loc[6, "destination_balance_error"] == pytest.approx(0)   # DEBIT dest: old + amount
    assert t.loc[2, "expected_new_balance_dest"] == 1500                                                              # TRANSFER dest: old + amount
    assert t.loc[13, "origin_balance_error"] == pytest.approx(0.03) and t.loc[13, "origin_balance_consistent"] == False   # noqa: E712
    assert t.loc[12, "origin_balance_error"] == pytest.approx(0.01) and t.loc[12, "origin_balance_consistent"] == True    # noqa: E712


def test_merchant_destinations_are_not_applicable_and_unrecorded_customer_destinations_are_insufficient(valid):
    t = q(valid, "SELECT * FROM silver.transactions").set_index("source_row_number")
    assert t.loc[1, "destination_balance_status"] == "NOT_APPLICABLE" and pd.isna(t.loc[1, "expected_new_balance_dest"])
    assert pd.isna(t.loc[1, "destination_balance_error"]) and pd.isna(t.loc[1, "destination_balance_consistent"])
    assert t.loc[9, "destination_balance_status"] == "INSUFFICIENT_EVIDENCE" and pd.isna(t.loc[9, "destination_balance_error"])
    assert t.loc[14, "destination_balance_status"] == "CONSISTENT" and t.loc[14, "origin_balance_status"] == "INSUFFICIENT_EVIDENCE"


def test_the_tolerance_boundary_is_the_configured_one(data_dir):
    tol = config.settings()["paysim"]["balance_tolerance"]
    rows = [(1, "PAYMENT", 10.0, "C1", 100.0, 90.0 + tol - 0.0005, "M1", 0.0, 0.0, 0, 0),
            (1, "PAYMENT", 10.0, "C2", 100.0, 90.0 + tol + 0.005, "M1", 0.0, 0.0, 0, 0)]
    ds = build(data_dir, rows)
    t = q(ds, "SELECT origin_balance_status FROM silver.transactions ORDER BY source_row_number")
    assert list(t["origin_balance_status"]) == ["CONSISTENT", "INCONSISTENT"]


# ---- fraud metrics ---------------------------------------------------------------------------------------------------------------------------
def test_confusion_metrics_on_known_numbers_and_zero_denominators():
    m = fraud.confusion_metrics(tp=8, fp=2, tn=985, fn=5)
    assert m["precision"] == pytest.approx(0.8) and m["recall"] == pytest.approx(8 / 13)
    assert m["f1"] == pytest.approx(2 * 0.8 * (8 / 13) / (0.8 + 8 / 13))
    assert m["false_positive_rate"] == pytest.approx(2 / 987) and m["prevalence"] == pytest.approx(13 / 1000)
    none = fraud.confusion_metrics(0, 0, 10, 0)
    assert none["precision"] is None and none["recall"] is None and none["f1"] is None


def test_the_operational_flag_is_scored_against_the_label_on_the_hand_built_file(valid):
    p = q(valid, "SELECT * FROM gold.fraud_flag_performance").set_index("scope")
    allr = p.loc["ALL"]
    assert (allr["tp"], allr["fp"], allr["fn"], allr["tn"]) == (1, 1, 2, 11)             # rows 9 / 10 / 2 and 15 / the rest
    assert allr["precision"] == pytest.approx(0.5) and allr["recall"] == pytest.approx(1 / 3) and allr["f1"] == pytest.approx(0.4)
    assert p.loc["TRANSFER", "tp"] == 1 and p.loc["PAYMENT", "tn"] == 5
    outcomes = q(valid, "SELECT source_row_number, flag_outcome FROM gold.fact_transactions WHERE flag_outcome <> 'TN' ORDER BY 1")
    assert dict(zip(outcomes["source_row_number"], outcomes["flag_outcome"], strict=True)) == {2: "FN", 9: "TP", 10: "FP", 15: "FN"}


# ---- features --------------------------------------------------------------------------------------------------------------------------------
def test_features_guard_against_division_by_zero_nulls_and_infinities(valid):
    con = connect(valid, read_only=True)
    try:
        f = con.execute(f"SELECT source_row_number, {ml._select(features.FEATURE_NAMES)} FROM gold.fact_transactions ORDER BY 1").df().set_index("source_row_number")
    finally:
        con.close()
    assert np.isfinite(f.to_numpy(dtype="float64")).all()
    g = q(valid, "SELECT * FROM gold.fact_transactions").set_index("source_row_number")
    assert pd.isna(g.loc[5, "amount_to_origin_balance_ratio"]) and f.loc[5, "amount_to_origin_balance_ratio"] == -1   # balance 0: no ratio, sentinel -1
    assert g.loc[2, "amount_to_origin_balance_ratio"] == pytest.approx(1.0) and g.loc[1, "amount_to_origin_balance_ratio"] == pytest.approx(0.1)
    assert g.loc[1, "log_amount"] == pytest.approx(math.log(101))
    assert g.loc[2, "origin_balance_delta"] == -500 and g.loc[2, "destination_balance_delta"] == 500
    assert bool(g.loc[2, "origin_fully_drained"]) and not bool(g.loc[1, "origin_fully_drained"]) and not bool(g.loc[5, "origin_fully_drained"])
    assert f.loc[5, "origin_balance_evidence"] == 0 and f.loc[2, "origin_balance_evidence"] == 1 and f.loc[3, "origin_balance_inconsistent"] == 1
    assert f.loc[1, "destination_is_merchant"] == 1 and f.loc[1, "type_PAYMENT"] == 1 and f.loc[1, "type_TRANSFER"] == 0
    assert f.loc[5, "origin_balance_error"] == 0                                              # NULL error -> 0, flagged by the evidence column


def test_the_registry_the_sql_and_the_ml_columns_agree(valid, monkeypatch):
    assert set(features.FEATURE_EXPR) == set(features.FEATURE_NAMES) and len(features.FEATURE_NAMES) == len(set(features.FEATURE_NAMES))
    assert set(features.PRE_TXN_NAMES) <= set(features.FEATURE_NAMES)
    pre_sources = {f.source for f in features.FEATURES if f.pre_txn}
    assert not any(s in ("new_balance_org", "new_balance_dest", "origin_balance_after", "origin_balance_delta", "origin_balance_error") for s in pre_sources)
    monkeypatch.setattr(ml, "verify_matrix_not_target", lambda x, y: None)   # on this 15-row file "drained" IS the label; the guard is tested below
    con = connect(valid, read_only=True)
    try:
        x, y = ml.load_split(con, 0, 10)
    finally:
        con.close()
    assert list(x.columns) == features.FEATURE_NAMES and len(x) == len(y) == 15


# ---- Gold ------------------------------------------------------------------------------------------------------------------------------------
def test_gold_tables_agree_with_each_other_and_with_silver(valid):
    fact = q(valid, "SELECT count(*) n, sum(amount) a, sum(is_fraud) f FROM gold.fact_transactions").iloc[0]
    assert (fact["n"], fact["f"]) == (15, 3)
    ts = q(valid, "SELECT sum(transactions) n, sum(total_amount) a, sum(fraud_transactions) f FROM gold.transaction_type_summary").iloc[0]
    assert (ts["n"], ts["a"], ts["f"]) == (fact["n"], pytest.approx(fact["a"]), fact["f"])
    dd = q(valid, "SELECT sum(transactions) n, sum(fraud_transactions) f, sum(flag_tp) tp, sum(flag_fp) fp, sum(flag_fn) fn FROM gold.fraud_daily_summary").iloc[0]
    assert (dd["n"], dd["f"], dd["tp"], dd["fp"], dd["fn"]) == (15, 3, 1, 1, 2)
    bq = q(valid, "SELECT sum(transactions) n FROM gold.balance_quality").iloc[0]
    assert bq["n"] == 15
    assert set(q(valid, "SELECT source_row_number FROM gold.fact_fraud")["source_row_number"]) == {2, 9, 10, 15}      # fraud or flagged
    acc = q(valid, "SELECT account, transactions, amount_sent, amount_received, fraud_transactions FROM gold.account_activity").set_index("account")
    assert acc.loc["M1", "transactions"] == 2 and acc.loc["M1", "amount_received"] == 200                            # rows 1 and its duplicate
    assert "C15" not in acc.index and "C2" in acc.index and acc.loc["C2", "fraud_transactions"] == 1                # single, non-fraud accounts are excluded
    fa = q(valid, "SELECT account, fraud_as_origin, fraud_as_destination, received_then_sent_fraud_funds FROM gold.fraud_account_summary").set_index("account")
    assert set(fa.index) == {"C2", "C3", "C12", "C13", "C21", "C22"}
    assert not fa["received_then_sent_fraud_funds"].any()


def test_every_gold_fact_column_has_a_role_and_no_label_column_is_a_feature_source(valid):
    cols = [r[0] for r in q(valid, "DESCRIBE gold.fact_transactions").to_numpy()]
    roles = features.classify_columns(cols)
    assert roles["is_fraud"] == "label" and roles["flag_outcome"] == "label_derived"
    assert all(roles[f.source] == "feature_source" for f in features.FEATURES)


def test_the_dashboard_queries_run_against_the_gold_tables(valid):
    con = connect(valid, read_only=True)
    try:
        for name, sql in queries.DASHBOARD_QUERIES.items():
            con.execute(sql).fetchall(), name
        p = queries.account_filter_params("C2", "", "", "fraud", 1, 10)
        assert len(con.execute(queries.ACCOUNT_FILTER, p).fetchall()) == 1
        assert len(con.execute(queries.ACCOUNT_HISTORY, ["C3"] * 6).fetchall()) == 2      # received from C2, sent to C4
    finally:
        con.close()


def test_gold_and_silver_are_reproducible_across_runs(data_dir):
    a = build(data_dir, VALID, name="a")
    b = build(data_dir, VALID, name="b")
    ha = q(a, "SELECT sum(hash(t)) h FROM (SELECT * FROM gold.fact_transactions) t").iloc[0, 0]
    hb = q(b, "SELECT sum(hash(t)) h FROM (SELECT * FROM gold.fact_transactions) t").iloc[0, 0]
    assert ha == hb


# ---- profiling and export ------------------------------------------------------------------------------------------------------------------------
def test_the_profile_is_computed_from_the_data(data_dir):
    ds = Dataset("t", write_csv(data_dir / "t.csv", VALID))
    p = profile.profile_source(ds)
    assert p["rows"] == 15 and p["exact_duplicate_rows"] == 1 and p["missing_columns"] == []
    assert p["fraud"]["fraud_transactions"] == 3 and p["fraud"]["flagged_transactions"] == 2 and p["fraud"]["flagged_and_fraud"] == 1
    assert p["fraud"]["fraud_rate"] == pytest.approx(3 / 15)
    assert p["amount"]["minimum"] == 0 and p["amount"]["maximum"] == 300000 and p["amount"]["total"] == pytest.approx(sum(r[2] for r in VALID))
    by = {t["type"]: t for t in p["transaction_types"]}
    assert by["PAYMENT"]["transactions"] == 5 and by["TRANSFER"]["fraud"] == 3
    assert p["accounts"]["origin_accounts"] == 14 and p["accounts"]["merchant_destinations"] == 4
    assert p["columns"]["nameOrig"]["nulls"] == 0 and p["columns"]["amount"]["inferred_type"] == "DOUBLE"
    assert json.loads((ds.dir / "profile.json").read_text(encoding="utf-8"))["rows"] == 15


def test_export_writes_parquet_and_csv_and_skips_csv_over_the_limit(valid, data_dir):
    m = export.export_gold(valid, csv_row_limit=100, out=data_dir / "exp")
    assert m["tables"]["fact_transactions"]["rows"] == 15 and m["tables"]["fact_transactions"]["csv"] == "fact_transactions.csv"
    assert len(pd.read_csv(data_dir / "exp" / "fact_transactions.csv")) == 15
    assert len(pd.read_parquet(data_dir / "exp" / "fact_transactions.parquet")) == 15
    m2 = export.export_gold(valid, csv_row_limit=10, out=data_dir / "exp2")
    assert m2["tables"]["fact_transactions"]["csv"] is None and "exceed" in m2["tables"]["fact_transactions"]["csv_skipped"]
    assert not (data_dir / "exp2" / "fact_transactions.csv").exists() and (data_dir / "exp2" / "fact_transactions.parquet").exists()
    assert "Power BI Desktop" in m["note"]


# ---- leakage guard ---------------------------------------------------------------------------------------------------------------------------
def test_the_label_and_anything_derived_from_it_can_never_be_a_feature():
    features.assert_no_leakage(features.FEATURE_NAMES)
    for bad in ("is_fraud", "isFraud", "flag_outcome", "fraud_score", "account_fraud_count", "label", "target_encoded", "outcome_x"):
        with pytest.raises(features.LeakageError):
            features.assert_no_leakage([*features.FEATURE_NAMES, bad])
    assert "is_flagged_fraud" in features.FEATURE_NAMES and not features.suspicious_name("is_flagged_fraud")    # the rule flag is allowed by name
    with pytest.raises(features.LeakageError, match="registry"):
        features.assert_no_leakage([*features.FEATURE_NAMES, "brand_new_column"])
    with pytest.raises(features.LeakageError, match="unclassified"):
        features.classify_columns(["amount", "some_new_gold_column"])


def test_a_label_derived_name_is_rejected_even_when_someone_registers_it(monkeypatch):
    for leaky in ("flag_outcome", "is_fraud", "fraud_rate_by_account"):
        monkeypatch.setattr(features, "FEATURE_NAMES", [*features.FEATURE_NAMES, leaky])           # registered, so only the name rule can catch it
        with pytest.raises(features.LeakageError, match="target-derived"):
            features.assert_no_leakage(features.FEATURE_NAMES)
        monkeypatch.undo()


def test_a_feature_that_reproduces_the_label_is_rejected_on_the_data():
    y = pd.Series([0, 1, 0, 1, 1], dtype="int8")
    x = pd.DataFrame({"a": [1, 2, 3, 4, 5], "b": [0, 1, 0, 1, 1]})
    with pytest.raises(features.LeakageError, match="identical"):
        features.verify_matrix_not_target(x, y)
    with pytest.raises(features.LeakageError, match="identical"):
        features.verify_matrix_not_target(x.assign(b=1 - y), y)
    features.verify_matrix_not_target(x.drop(columns="b"), y)


def test_load_split_refuses_a_tampered_feature_list(valid, monkeypatch):
    monkeypatch.setattr(ml, "FEATURE_NAMES", [*features.FEATURE_NAMES, "flag_outcome"])
    con = connect(valid, read_only=True)
    try:
        with pytest.raises(features.LeakageError):
            ml.load_split(con, 0, 10)
    finally:
        con.close()


# ---- synthetic file: sampling, temporal split, models, pipeline ---------------------------------------------------------------------------------
def synthetic(path, n=6000, seed=7):
    rng = np.random.default_rng(seed)
    step = np.sort(rng.integers(1, 101, n))
    types = rng.choice(["PAYMENT", "CASH_IN", "CASH_OUT", "TRANSFER", "DEBIT"], n, p=[0.40, 0.20, 0.22, 0.14, 0.04])
    amount = np.round(rng.lognormal(8, 1.2, n), 2)
    rows = []
    for i in range(n):
        t, a = types[i], float(amount[i])
        fraud = int(t in ("TRANSFER", "CASH_OUT") and rng.random() < 0.07)
        drains = (fraud and rng.random() < 0.9) or (t in ("TRANSFER", "CASH_OUT") and not fraud and rng.random() < 0.03)   # overlap on purpose
        old = a if drains else round(a * float(rng.uniform(1.2, 9)), 2)
        if t == "CASH_IN":
            new = round(old + a, 2)
        else:
            new = round(old - a, 2)
        dest = f"M{rng.integers(1, 400)}" if t == "PAYMENT" else f"C{rng.integers(1000, 4000)}"
        od = 0.0 if dest.startswith("M") else round(float(rng.uniform(0, 50000)) + (a if t == "CASH_IN" else 0.0), 2)   # an agent can pay out
        credited = not (fraud and rng.random() < 0.9) and not (not fraud and rng.random() < 0.03)   # overlap on purpose
        nd = 0.0 if dest.startswith("M") else round(od + (-a if t == "CASH_IN" else a), 2) if credited else od
        flagged = int(t == "TRANSFER" and a > 200000 and fraud)
        rows.append((int(step[i]), t, a, f"C{rng.integers(1, 5000)}", old, new, dest, od, nd, fraud, flagged))
    return write_csv(path, rows)


@pytest.fixture()
def synth(data_dir):
    return Dataset("s", synthetic(data_dir / "s.csv"))


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_the_development_sample_is_exact_stratified_traceable_and_reproducible(synth, data_dir):
    m1 = sample.make_dev_sample(synth, out_path=data_dir / "a" / "s.parquet", rows=1000)
    m2 = sample.make_dev_sample(synth, out_path=data_dir / "b" / "s.parquet", rows=1000)
    assert m1["sample_rows"] == 1000 and m1["sha256"] == m2["sha256"] == sha(data_dir / "b" / "s.parquet")   # same seed, same bytes
    src = pd.read_csv(synth.source)
    src_fraud = src.groupby("type")["isFraud"].sum()
    frac = config.settings()["paysim"]["dev_fraud_fraction"]
    for t, f in m1["by_type"].items():
        assert abs(f["fraud"] - round(frac * src_fraud.get(t, 0))) <= 1, t                                    # fraud strata keep the configured share
    assert m1["sample_fraud_rate"] > m1["source_fraud_rate"] and m1["fraud_over_representation"] > 1
    s = pd.read_parquet(data_dir / "a" / "s.parquet")
    assert s["source_row_number"].is_monotonic_increasing and s["source_row_number"].is_unique
    joined = s.merge(src.reset_index().assign(source_row_number=lambda d: d["index"] + 1), on="source_row_number", suffixes=("_s", ""))
    assert len(joined) == 1000 and (joined["amount_s"] == joined["amount"]).all() and (joined["nameOrig_s"] == joined["nameOrig"]).all()   # provenance
    meta = json.loads((data_dir / "a" / "s.meta.json").read_text(encoding="utf-8"))
    assert meta["seed"] == config.settings()["paysim"]["seed"] and meta["source_rows"] == len(src)


def test_the_temporal_split_never_lets_the_future_into_training(synth):
    pipeline.run_pipeline(synth, with_profile=False, with_ml=False, with_export=False)
    con = connect(synth, read_only=True)
    try:
        b = ml.split_bounds(con)
        assert b["min_step"] - 1 < b["train_end"] < b["validation_end"] < b["max_step"]
        tr, va, te = (ml.load_split(con, lo, hi)[0] for lo, hi in ((b["min_step"] - 1, b["train_end"]), (b["train_end"], b["validation_end"]),
                                                                   (b["validation_end"], b["max_step"])))
        steps = {k: con.execute(f"SELECT min(step), max(step) FROM gold.fact_transactions WHERE step > {lo} AND step <= {hi}").fetchone()
                 for k, (lo, hi) in {"tr": (0, b["train_end"]), "va": (b["train_end"], b["validation_end"]), "te": (b["validation_end"], b["max_step"])}.items()}
        assert steps["tr"][1] < steps["va"][0] and steps["va"][1] < steps["te"][0]                              # strictly ordered, no overlap
        assert len(tr) + len(va) + len(te) == 6000
    finally:
        con.close()


def test_models_train_evaluate_deterministically_and_report_imbalance_aware_metrics(synth):
    pipeline.run_pipeline(synth, with_profile=False, with_ml=False, with_export=False)
    a = ml.train_and_evaluate(synth)
    b = ml.train_and_evaluate(synth)
    for name in a["metrics"]:                                                                                  # reproducible: counts exactly,
        ta, tb = a["metrics"][name]["test"], b["metrics"][name]["test"]                                        # floats to 1e-9 (parallel tree averaging
        for key, va in ta.items():                                                                             # can differ in the last digits)
            assert va == pytest.approx(tb[key], rel=1e-9, abs=1e-12) if isinstance(va, float) else va == tb[key], (name, key)
    assert set(a["metrics"]) == {"logistic_regression", "random_forest", "logistic_regression_pre_txn", "random_forest_pre_txn",
                                 "operational_flag_isFlaggedFraud"}
    for name, d in a["metrics"].items():
        t = d["test"]
        assert {"tp", "fp", "tn", "fn", "precision", "recall", "f1"} <= set(t), name
        assert t["tp"] + t["fp"] + t["tn"] + t["fn"] == a["run"]["test_rows"]
    assert a["metrics"]["random_forest"]["test"]["pr_auc"] > 0.8                                                 # the synthetic fraud is learnable
    assert a["audit_flagged"] is not None and a["run"]["test_fraud"] > 0 and a["run"]["validation_fraud"] > 0
    m = q(synth, "SELECT model, split FROM gold.model_metrics")
    assert len(m) == len(a["metrics"]) * 2 - 1                                                                    # validation+test each, flag: test only
    assert len(q(synth, "SELECT * FROM gold.model_feature_audit")) == len(features.FEATURE_NAMES)
    assert not any(features.suspicious_name(f) for f in q(synth, "SELECT feature FROM gold.model_feature_importance")["feature"] if f != "is_flagged_fraud")


def test_the_whole_pipeline_records_time_and_memory_per_stage_and_is_idempotent(synth):
    r1 = pipeline.run_pipeline(synth)
    assert [s["stage"] for s in r1["stages"]] == ["profile", "bronze", "quality_and_silver", "gold", "ml", "export"]
    assert all(s["seconds"] >= 0 and s["rows"] for s in r1["stages"]) and r1["conserved"]
    assert r1["rows"]["bronze"] == 6000 == r1["rows"]["silver"] + r1["rows"]["quarantined"]
    h1 = q(synth, "SELECT sum(hash(t)) FROM (SELECT * FROM gold.fact_transactions) t").iloc[0, 0]
    r2 = pipeline.run_pipeline(synth)
    h2 = q(synth, "SELECT sum(hash(t)) FROM (SELECT * FROM gold.fact_transactions) t").iloc[0, 0]
    assert h1 == h2 and r2["rows"] == r1["rows"]
    assert json.loads((synth.dir / "run_metrics.json").read_text(encoding="utf-8"))["total_seconds"] > 0


def test_the_scaling_path_is_the_same_code_for_any_source_size(data_dir):
    small = Dataset("small", synthetic(data_dir / "small.csv", n=800, seed=1))
    large = Dataset("large", synthetic(data_dir / "large.csv", n=3000, seed=2))
    for ds in (small, large):
        r = pipeline.run_pipeline(ds, with_profile=False, with_ml=False, with_export=False)
        assert r["rows"]["gold.fact_transactions"] == r["rows"]["silver"]
    assert duckdb is not None
