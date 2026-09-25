"""Fraud-signal model: merchant-day anomaly detection on TRUSTED data only.

Why merchant-day: card-testing bursts, velocity spikes and refund spikes all show up as a merchant behaving
very differently from its own history on one day. Features are z-scores against each merchant's own
expanding baseline (previous days only, so there is no look-ahead leakage).

Trust boundary: features are built from Silver (rows that passed data quality) and the model refuses to run
if the most recent pipeline run was stopped by the quality gate.
"""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from . import config, db

FEATURES = ["n_payments", "n_failed", "n_small_failed", "n_refunds", "mean_amount", "max_amount"]
Z_FEATURES = [f"z_{f}" for f in FEATURES]
Z_CLIP = 10.0


class BlockedByQualityGate(RuntimeError):
    pass


def latest_gate(con) -> tuple[str | None, str | None]:
    row = con.execute("SELECT gate, status FROM ops.run_log ORDER BY started_at DESC LIMIT 1").fetchone()
    return (row[0], row[1]) if row else (None, None)


def build_features(con) -> pd.DataFrame:
    daily = con.execute("""
        SELECT merchant_id, cast(created_at AS DATE) AS d,
               count(*) FILTER (WHERE txn_type = 'payment') AS n_payments,
               count(*) FILTER (WHERE txn_type = 'payment' AND status = 'failed') AS n_failed,
               count(*) FILTER (WHERE txn_type = 'payment' AND status = 'failed' AND amount < 20) AS n_small_failed,
               count(*) FILTER (WHERE txn_type = 'refund') AS n_refunds,
               coalesce(avg(amount) FILTER (WHERE txn_type = 'payment' AND status = 'captured'), 0) AS mean_amount,
               coalesce(max(amount) FILTER (WHERE txn_type = 'payment'), 0) AS max_amount
        FROM silver.gateway_txns GROUP BY 1, 2""").df()
    if daily.empty:
        return daily
    merchants = con.execute("SELECT DISTINCT merchant_id FROM silver.merchants_scd2").df()
    daily["d"] = pd.to_datetime(daily["d"])
    days = pd.DataFrame({"d": pd.date_range(daily["d"].min(), daily["d"].max())})
    grid = merchants.merge(days, how="cross")
    df = grid.merge(daily, on=["merchant_id", "d"], how="left").fillna(0).sort_values(["merchant_id", "d"])
    df = df.reset_index(drop=True)
    for f in FEATURES:
        g = df.groupby("merchant_id")[f]
        mu = g.transform(lambda s: s.expanding().mean().shift(1))
        sd = g.transform(lambda s: s.expanding().std().shift(1)).fillna(0)
        floor = np.maximum(1.0, 0.1 * mu.abs().fillna(0)) if f.endswith("amount") else 1.0
        df[f"z_{f}"] = ((df[f] - mu) / np.maximum(sd, floor)).clip(-Z_CLIP, Z_CLIP)
    df["history_days"] = df.groupby("merchant_id").cumcount()
    df["d"] = df["d"].dt.date
    return df


def score(df: pd.DataFrame, contamination: float, min_history: int, seed: int = 42) -> pd.DataFrame:
    """Isolation Forest on the z-features, plus a transparent rule baseline for comparison."""
    df = df.copy()
    elig = df["history_days"] >= min_history
    df["eligible"] = elig
    df["iforest_score"] = np.nan
    df["iforest_flag"] = False
    if elig.sum() > 50:
        X = df.loc[elig, Z_FEATURES].to_numpy()
        model = IsolationForest(n_estimators=200, contamination=contamination, random_state=seed).fit(X)
        df.loc[elig, "iforest_score"] = -model.decision_function(X)
        df.loc[elig, "iforest_flag"] = model.predict(X) == -1
    upper = df[["z_n_payments", "z_n_small_failed", "z_n_refunds"]].max(axis=1)
    df["rule_flag"] = elig & (upper >= 4.0)
    return df


def evaluate_scores(df: pd.DataFrame, truth: pd.DataFrame) -> dict:
    truth = truth[truth["merchant_id"] != ""].copy()
    truth["key"] = list(zip(truth["merchant_id"], pd.to_datetime(truth["date"]).dt.date, strict=True))
    df = df.copy()
    df["key"] = list(zip(df["merchant_id"], df["d"], strict=True))
    elig_keys = set(df.loc[df["eligible"], "key"])
    positives = set(truth["key"]) & elig_keys
    out: dict = {"eligible_merchant_days": int(df["eligible"].sum()), "true_anomalies_eligible": len(positives)}
    for name in ("iforest_flag", "rule_flag"):
        flagged = set(df.loc[df[name], "key"])
        tp = len(flagged & positives)
        out[name] = {"flagged": len(flagged), "tp": tp,
                     "precision": round(tp / len(flagged), 4) if flagged else None,
                     "recall": round(tp / len(positives), 4) if positives else None}
    flagged = set(df.loc[df["iforest_flag"], "key"])
    pat = {}
    for p, grp in truth.groupby("pattern"):
        keys = set(grp["key"]) & elig_keys
        pat[p] = {"n": len(keys), "recall": round(len(keys & flagged) / len(keys), 4) if keys else None}
    out["iforest_recall_by_pattern"] = pat
    return out


def run_anomaly_model(db_path=None, truth_dir=None, contamination: float | None = None,
                      enforce_gate: bool = True) -> dict:
    cfg = config.settings()["anomaly"]
    contamination = contamination if contamination is not None else cfg["contamination"]
    con = db.connect(db_path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS ml")
        gate, status = latest_gate(con)
        if enforce_gate and (gate == "HALT" or status in ("QUALITY_GATE_FAILED", "FAILED")):
            return {"status": "BLOCKED_BY_QUALITY_GATE", "latest_run_status": status,
                    "detail": "model not refreshed: latest batch did not pass the quality gate"}
        feats = build_features(con)
        if feats.empty:
            return {"status": "NO_DATA"}
        scored = score(feats, contamination, cfg["min_history_days"])
        con.register("scored", scored[["merchant_id", "d", *FEATURES, *Z_FEATURES, "eligible", "iforest_score",
                                       "iforest_flag", "rule_flag"]])
        con.execute("CREATE OR REPLACE TABLE ml.anomaly_scores AS SELECT * FROM scored")
        con.unregister("scored")
        result: dict = {"status": "OK", "contamination": contamination,
                        "flagged_merchant_days": int(scored["iforest_flag"].sum())}
        tpath = (truth_dir or config.truth_dir()) / "anomaly.parquet"
        if tpath.exists():
            result["evaluation_vs_ground_truth"] = evaluate_scores(scored, pd.read_parquet(tpath))
            ev = result["evaluation_vs_ground_truth"]["iforest_flag"]
            now = datetime.now()
            for metric in ("precision", "recall"):
                if ev[metric] is not None:
                    con.execute("INSERT INTO ops.model_metrics VALUES (?,?,?,?,?)",
                                [now, "merchant_day_anomaly_iforest", metric, ev[metric], None])
        return result
    finally:
        con.close()
