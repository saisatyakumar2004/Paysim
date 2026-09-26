"""Fraud metrics. Fraud is ~0.13 % of the full file, so accuracy is meaningless (predicting "never fraud" scores 99.87 %): every
function here reports the confusion matrix, precision, recall and F1."""

from __future__ import annotations


def confusion_metrics(tp: int, fp: int, tn: int, fn: int) -> dict:
    """Confusion matrix and the metrics that matter for an imbalanced target. None where the denominator is 0."""
    def ratio(a, b):
        return a / b if b else None

    p, r = ratio(tp, tp + fp), ratio(tp, tp + fn)
    f1 = ratio(2 * p * r, p + r) if p is not None and r is not None else None
    n = tp + fp + tn + fn
    return {"tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn), "precision": p, "recall": r, "f1": f1,
            "false_positive_rate": ratio(fp, fp + tn), "prevalence": ratio(tp + fn, n), "flagged_rate": ratio(tp + fp, n)}


def flag_performance(con, table: str = "gold.fact_transactions") -> list[dict]:
    """The operational flag (isFlaggedFraud) scored against the label (isFraud): overall and per transaction type."""
    rows = con.execute(f"""
        SELECT coalesce(type, 'ALL') AS scope,
               count(*) FILTER (WHERE is_flagged_fraud = 1 AND is_fraud = 1), count(*) FILTER (WHERE is_flagged_fraud = 1 AND is_fraud = 0),
               count(*) FILTER (WHERE is_flagged_fraud = 0 AND is_fraud = 0), count(*) FILTER (WHERE is_flagged_fraud = 0 AND is_fraud = 1)
        FROM {table} GROUP BY GROUPING SETS ((type), ()) ORDER BY 1""").fetchall()
    return [{"scope": s, **confusion_metrics(tp, fp, tn, fn)} for s, tp, fp, tn, fn in rows]
