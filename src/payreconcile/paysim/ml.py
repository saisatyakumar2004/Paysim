"""Baseline fraud models: logistic regression and random forest, on a TEMPORAL split.

Why temporal: transactions are ordered in time (`step`). A random split would let the model train on hours that come after the
hours it is tested on. Train = steps up to `train_end_fraction` of the step range, validation up to `validation_end_fraction`,
test = the rest. The decision threshold is chosen on validation only (maximum F1) and applied unchanged to test.

Training only: non-fraud rows are deterministically undersampled (`negatives_per_positive`, chosen by hash(source_row_number, seed))
so the fit is fast; validation and test keep EVERY row, so all reported metrics are on the true class balance. Reported: PR-AUC
(the metric that matters at this prevalence), ROC-AUC, precision, recall, F1 and the confusion matrix; never accuracy alone.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

from .common import Dataset, cfg, connect
from .features import (
    FEATURE_EXPR,
    FEATURE_NAMES,
    FEATURES,
    PRE_TXN_NAMES,
    TARGET,
    assert_no_leakage,
    verify_matrix_not_target,
)
from .fraud import confusion_metrics

NUMERIC = [f.name for f in FEATURES if f.kind == "numeric"]
BINARY = [f.name for f in FEATURES if f.kind != "numeric"]
# Two feature sets. "all" uses closing balances (observed after the transaction); "pre_txn" uses only what a real-time authoriser
# knows before the transaction completes. Reporting both keeps simulator artefacts in the closing balances from being read as skill.
VARIANTS = {"": FEATURE_NAMES, "_pre_txn": PRE_TXN_NAMES}


def _signed_log1p(x):
    return np.sign(x) * np.log1p(np.abs(x))


def _select(names: list[str]) -> str:
    return ", ".join(f"{FEATURE_EXPR[n]} AS {n}" for n in names)


def split_bounds(con) -> dict:
    m = cfg()["ml"]
    lo, hi = con.execute("SELECT min(step), max(step) FROM gold.fact_transactions").fetchone()
    span = hi - lo + 1
    train_end = lo - 1 + int(m["train_end_fraction"] * span)
    val_end = lo - 1 + int(m["validation_end_fraction"] * span)
    return {"min_step": int(lo), "max_step": int(hi), "train_end": int(train_end), "validation_end": int(val_end)}


def load_split(con, lo: int, hi: int, negative_keep: float | None = None, seed: int = 42) -> tuple[pd.DataFrame, pd.Series]:
    """Features (in registry order) and label for steps in (lo, hi]. `negative_keep` undersamples non-fraud rows (training only)."""
    assert_no_leakage(FEATURE_NAMES)
    keep = ""
    if negative_keep is not None and negative_keep < 1:
        keep = f" AND (is_fraud = 1 OR (hash(source_row_number, {seed}) % 1000000) < {int(negative_keep * 1000000)})"
    df = con.execute(f"SELECT {_select(FEATURE_NAMES)}, {TARGET} AS y FROM gold.fact_transactions "
                     f"WHERE step > {lo} AND step <= {hi}{keep} ORDER BY source_row_number").df()
    y = df.pop("y").astype("int8")
    x = df[FEATURE_NAMES].astype("float32")
    if not np.isfinite(x.to_numpy()).all():
        raise ValueError("non-finite values in the feature matrix")
    verify_matrix_not_target(x, y)
    return x, y


def _lr(seed: int, numeric: list[str], binary: list[str]) -> Pipeline:
    pre = ColumnTransformer([("num", Pipeline([("log", FunctionTransformer(_signed_log1p)), ("scale", StandardScaler())]), numeric),
                             ("bin", "passthrough", binary)])
    return Pipeline([("pre", pre), ("model", LogisticRegression(class_weight="balanced", max_iter=2000, random_state=seed))])


def _rf(seed: int, trees: int) -> RandomForestClassifier:
    return RandomForestClassifier(n_estimators=trees, min_samples_leaf=3, class_weight="balanced_subsample", n_jobs=4,
                                  random_state=seed)


def _best_threshold(y, score) -> float:
    if int(y.sum()) == 0:
        return 0.5
    p, r, t = precision_recall_curve(y, score)
    f1 = np.where(p[:-1] + r[:-1] > 0, 2 * p[:-1] * r[:-1] / np.maximum(p[:-1] + r[:-1], 1e-12), 0)
    return float(t[int(np.argmax(f1))])


def _scores(y, score, threshold: float) -> dict:
    pred = score >= threshold
    yy = y.to_numpy().astype(bool)
    m = confusion_metrics(int((pred & yy).sum()), int((pred & ~yy).sum()), int((~pred & ~yy).sum()), int((~pred & yy).sum()))
    m["pr_auc"] = float(average_precision_score(y, score)) if y.sum() else None
    m["roc_auc"] = float(roc_auc_score(y, score)) if 0 < y.sum() < len(y) else None
    m["threshold"] = threshold
    return m


def feature_audit(x: pd.DataFrame, y: pd.Series, flag_at: float) -> list[dict]:
    """Univariate predictive power of each input on the training rows. A value near 1 means the feature alone almost decides the
    label: not necessarily leakage, but worth a look (balances are observed after the transaction)."""
    out = []
    for c in x.columns:
        col = x[c].to_numpy()
        if y.sum() == 0 or np.ptp(col) == 0:
            out.append({"feature": c, "univariate_roc_auc": None, "univariate_pr_auc": None, "flagged": False})
            continue
        auc = float(roc_auc_score(y, col))
        auc = max(auc, 1 - auc)
        ap = float(max(average_precision_score(y, col), average_precision_score(y, -col)))
        out.append({"feature": c, "univariate_roc_auc": auc, "univariate_pr_auc": ap, "flagged": auc >= flag_at})
    return out


def train_and_evaluate(ds: Dataset) -> dict:
    m = cfg()["ml"]
    seed = int(m["random_state"])
    t0 = time.perf_counter()
    con = connect(ds)
    try:
        b = split_bounds(con)
        n_pos, n_neg = con.execute(f"SELECT count(*) FILTER (WHERE is_fraud = 1), count(*) FILTER (WHERE is_fraud = 0) FROM gold.fact_transactions "
                                   f"WHERE step <= {b['train_end']}").fetchone()
        keep = min(1.0, int(m["negatives_per_positive"]) * max(int(n_pos), 1) / max(int(n_neg), 1))
        xtr, ytr = load_split(con, b["min_step"] - 1, b["train_end"], keep, seed)
        xva, yva = load_split(con, b["train_end"], b["validation_end"])
        xte, yte = load_split(con, b["validation_end"], b["max_step"])
        flag_test = con.execute(f"SELECT count(*) FILTER (WHERE is_flagged_fraud = 1 AND is_fraud = 1), count(*) FILTER (WHERE is_flagged_fraud = 1 AND is_fraud = 0), "
                                f"count(*) FILTER (WHERE is_flagged_fraud = 0 AND is_fraud = 0), count(*) FILTER (WHERE is_flagged_fraud = 0 AND is_fraud = 1) "
                                f"FROM gold.fact_transactions WHERE step > {b['validation_end']}").fetchone()
        metrics: dict[str, dict] = {}
        importance: list[dict] = []
        curves: list[dict] = []
        for suffix, names in VARIANTS.items():
            num = [n for n in NUMERIC if n in names]
            binr = [n for n in BINARY if n in names]
            models = {"logistic_regression" + suffix: _lr(seed, num, binr), "random_forest" + suffix: _rf(seed, int(m["rf_trees"]))}
            for name, model in models.items():
                model.fit(xtr[names], ytr)
                sva = model.predict_proba(xva[names])[:, 1]
                ste = model.predict_proba(xte[names])[:, 1]
                thr = _best_threshold(yva, sva)
                metrics[name] = {"validation": _scores(yva, sva, thr), "test": _scores(yte, ste, thr)}
                if yte.sum():
                    p, r, t = precision_recall_curve(yte, ste)
                    idx = np.unique(np.linspace(0, len(t) - 1, min(200, len(t))).astype(int)) if len(t) else []
                    curves += [{"model": name, "threshold": float(t[i]), "precision": float(p[i]), "recall": float(r[i])} for i in idx]
                if name.startswith("random_forest"):
                    imp, order = model.feature_importances_, names
                else:
                    imp, order = np.abs(model.named_steps["model"].coef_[0]), num + binr
                importance += [{"model": name, "feature": f, "importance": float(v)} for f, v in zip(order, imp, strict=True)]
        metrics["operational_flag_isFlaggedFraud"] = {"test": {**confusion_metrics(*flag_test), "pr_auc": None, "roc_auc": None, "threshold": None}}
        audit = feature_audit(xtr, ytr, float(m["audit_flag_auc"]))
        run = {**b, "seed": seed, "train_rows_used": int(len(xtr)), "train_fraud": int(ytr.sum()), "train_negative_keep_fraction": keep,
               "validation_rows": int(len(xva)), "validation_fraud": int(yva.sum()), "test_rows": int(len(xte)), "test_fraud": int(yte.sum()),
               "features": len(FEATURE_NAMES), "seconds": round(time.perf_counter() - t0, 2)}
        _store(con, metrics, importance, curves, audit, run)
    finally:
        con.close()
    return {"run": run, "metrics": metrics, "audit_flagged": [a["feature"] for a in audit if a["flagged"]]}


def _store(con, metrics, importance, curves, audit, run) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("CREATE OR REPLACE TABLE gold.model_metrics (model VARCHAR, split VARCHAR, threshold DOUBLE, tp BIGINT, fp BIGINT, tn BIGINT, fn BIGINT, "
                "precision DOUBLE, recall DOUBLE, f1 DOUBLE, pr_auc DOUBLE, roc_auc DOUBLE, false_positive_rate DOUBLE, prevalence DOUBLE)")
    rows = [(mod, split, v["threshold"], v["tp"], v["fp"], v["tn"], v["fn"], v["precision"], v["recall"], v["f1"], v["pr_auc"], v["roc_auc"],
             v["false_positive_rate"], v["prevalence"]) for mod, d in metrics.items() for split, v in d.items()]
    con.executemany("INSERT INTO gold.model_metrics VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.execute("CREATE OR REPLACE TABLE gold.model_feature_importance (model VARCHAR, feature VARCHAR, importance DOUBLE)")
    con.executemany("INSERT INTO gold.model_feature_importance VALUES (?,?,?)", [(r["model"], r["feature"], r["importance"]) for r in importance])
    con.execute("CREATE OR REPLACE TABLE gold.model_pr_curve (model VARCHAR, threshold DOUBLE, precision DOUBLE, recall DOUBLE)")
    con.executemany("INSERT INTO gold.model_pr_curve VALUES (?,?,?,?)", [(r["model"], r["threshold"], r["precision"], r["recall"]) for r in curves])
    con.execute("CREATE OR REPLACE TABLE gold.model_feature_audit (feature VARCHAR, univariate_roc_auc DOUBLE, univariate_pr_auc DOUBLE, flagged BOOLEAN)")
    con.executemany("INSERT INTO gold.model_feature_audit VALUES (?,?,?,?)",
                    [(a["feature"], a["univariate_roc_auc"], a["univariate_pr_auc"], a["flagged"]) for a in audit])
    con.execute("CREATE OR REPLACE TABLE gold.model_run (key VARCHAR, value VARCHAR)")
    con.executemany("INSERT INTO gold.model_run VALUES (?,?)", [(k, str(v)) for k, v in run.items()])
