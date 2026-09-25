"""ML evaluation and leakage controls for the merchant-day anomaly model.

Prediction time = end of the merchant-day. Feature cutoff = previous days only. Inputs = gateway transactions only.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from payreconcile import anomaly

START = date(2026, 1, 1)


def seed_history(con, days: int, spike_day: int | None = None, spike_size: int = 60) -> None:
    """3 merchants with ~10 payments/day; optionally one merchant explodes on `spike_day`."""
    rng = np.random.default_rng(5)
    rows = []
    n = 0
    for d in range(days):
        for m in ("M1", "M2", "M3"):
            count = int(rng.poisson(10)) + (spike_size if (m == "M1" and d == spike_day) else 0)
            for _ in range(count):
                n += 1
                rows.append((f"T{n}", f"O{n}", m, float(rng.integers(100, 900)), "INR", "captured", "upi", "payment", None,
                             datetime.combine(START + timedelta(days=d), datetime.min.time()) + timedelta(hours=int(rng.integers(0, 24))),
                             "seed"))
    con.executemany("INSERT INTO silver.gateway_txns VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.execute("INSERT INTO silver.merchants_scd2 VALUES ('M3', 'Shop 3', 'food', 'Pune', 'low', 2.0, DATE '1970-01-01', "
                "DATE '9999-12-31', true, 'h3', 'seed')")


def features(con) -> pd.DataFrame:
    df = anomaly.build_features(con)
    return df.sort_values(["merchant_id", "d"]).reset_index(drop=True)


def test_a_days_features_do_not_change_when_later_days_arrive(con):
    """Feature cutoff: scoring day D must not depend on anything after D."""
    seed_history(con, days=20)
    con.execute("DELETE FROM silver.gateway_txns WHERE created_at >= TIMESTAMP '2026-01-16'")        # only days 1..15 exist
    early = features(con)
    con.execute("DELETE FROM silver.gateway_txns")
    seed_history(con, days=20)                                                                       # now the full 20 days
    full = features(con)
    cutoff = START + timedelta(days=14)
    a = early[early["d"] <= cutoff].reset_index(drop=True)
    b = full[full["d"] <= cutoff].reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b)


def test_the_baseline_a_day_is_compared_with_excludes_that_day(con):
    seed_history(con, days=15, spike_day=12, spike_size=200)
    df = features(con)
    m1 = df[df["merchant_id"] == "M1"].reset_index(drop=True)
    spike_row = m1[m1["d"] == START + timedelta(days=12)].iloc[0]
    prior = m1[m1["d"] < START + timedelta(days=12)]["n_payments"]
    expected = (spike_row["n_payments"] - prior.mean()) / max(prior.std(), 1.0)
    assert spike_row["z_n_payments"] == pytest.approx(min(expected, anomaly.Z_CLIP))
    assert spike_row["z_n_payments"] >= 4                                # and the spike itself is clearly visible


def test_features_ignore_settlements_reconciliation_and_orders(con):
    """Inputs: gateway transactions only. Scrambling every other table must not change a single feature."""
    seed_history(con, days=15)
    before = features(con)
    con.execute("""INSERT INTO silver.settlements VALUES ('S1', 'GW-T1', 'T1', 'M1', 999999, 1, 999998,
                   DATE '2026-01-05', true, 'x')""")
    con.execute("""INSERT INTO gold.fact_reconciliation (item_id, item_type, status, money_at_risk)
                   VALUES ('TXN:T1', 'txn', 'MISSING_SETTLEMENT', 12345)""")
    con.execute("""INSERT INTO silver.orders VALUES ('O1', 'M1', 'C1', 1, 'cancelled', TIMESTAMP '2026-01-03 10:00:00',
                   TIMESTAMP '2026-01-03 10:00:00', 'x')""")
    pd.testing.assert_frame_equal(before, features(con))
    assert set(anomaly.FEATURES) <= {"n_payments", "n_failed", "n_small_failed", "n_refunds", "mean_amount", "max_amount"}


def test_labels_are_read_only_after_scoring(con, tmp_path):
    """The ground truth must not influence which merchant-days are flagged."""
    seed_history(con, days=25, spike_day=20)
    df = features(con)
    with_labels = anomaly.score(df, contamination=0.02, min_history=7)
    truth = pd.DataFrame([{"merchant_id": "M1", "date": START + timedelta(days=20), "pattern": "velocity_burst"}])
    # scoring has no label argument at all; evaluating afterwards must not modify the scored frame
    snapshot = with_labels.copy()
    anomaly.evaluate_scores(with_labels, truth)
    pd.testing.assert_frame_equal(with_labels, snapshot)
    import inspect
    assert "truth" not in inspect.signature(anomaly.score).parameters


def test_the_model_refuses_to_refresh_after_a_halted_batch(con, data_dir):
    seed_history(con, days=15)
    con.execute("""INSERT INTO ops.run_log VALUES ('r1', DATE '2026-01-15', 1, now(), now(), 1.0, 'QUALITY_GATE_FAILED',
                   0.5, 'HALT', 10, 0, 10, NULL)""")
    con.close()
    out = anomaly.run_anomaly_model()
    assert out["status"] == "BLOCKED_BY_QUALITY_GATE"


def test_evaluation_metrics_precision_recall_f1_fp_fn_are_computed_correctly():
    days = [START + timedelta(days=i) for i in range(6)]
    df = pd.DataFrame({"merchant_id": ["M1"] * 6, "d": days, "eligible": True,
                       "iforest_flag": [True, True, False, False, False, False],       # flags days 0,1
                       "rule_flag": [True, False, False, False, False, False]})
    truth = pd.DataFrame({"merchant_id": ["M1", "M1", "M1"], "date": [days[0], days[2], days[3]], "pattern": ["a", "a", "b"]})
    ev = anomaly.evaluate_scores(df, truth)
    f = ev["iforest_flag"]
    assert (f["flagged"], f["tp"], f["fp"], f["fn"]) == (2, 1, 1, 2)
    assert f["precision"] == 0.5 and f["recall"] == pytest.approx(0.3333, abs=1e-4)
    assert f["f1"] == pytest.approx(0.4, abs=1e-4)                                        # 2*0.5*0.333/(0.5+0.333)
    r = ev["rule_flag"]
    assert (r["tp"], r["fp"], r["fn"], r["precision"]) == (1, 0, 2, 1.0)


def test_scoring_is_deterministic_for_a_fixed_seed(con):
    seed_history(con, days=25, spike_day=20)
    df = features(con)
    a = anomaly.score(df, contamination=0.02, min_history=7)
    b = anomaly.score(df, contamination=0.02, min_history=7)
    pd.testing.assert_frame_equal(a, b)
