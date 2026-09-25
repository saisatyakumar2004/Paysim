"""Quarantine persistence. Also records WHICH keys changed quarantine state, because reconciliation depends on it
(a settlement for a quarantined payment is not an orphan; a released payment must re-open its settlement)."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

COLUMNS = ["run_id", "run_date", "dataset", "natural_key", "order_id", "rule_ids", "reason", "raw_json", "quarantined_at"]


def _clean(v):
    return None if v is None or (isinstance(v, float) and v != v) else v


def replace(con, run_id: str, run_date: date, dataset: str, qdf: pd.DataFrame | None,
            keep_rules: tuple[str, ...] | None = None) -> int:
    """Replace this (run_date, dataset) slice of quarantine.rows with `qdf`.

    keep_rules: rows carrying one of these rule ids are left untouched (replay never rewrites duplicate/schema rejects).
    Returns the number of keys whose quarantine state changed (also appended to ops.change_log).
    """
    cond, params = "", []
    if keep_rules:
        cond = f" AND rule_ids NOT IN ({','.join('?' * len(keep_rules))})"
        params = list(keep_rules)
    old = {(_clean(k), _clean(o), r) for k, o, r in con.execute(
        f"SELECT natural_key, order_id, rule_ids FROM quarantine.rows WHERE run_date = ? AND dataset = ?{cond}",
        [run_date, dataset, *params]).fetchall()}
    con.execute(f"DELETE FROM quarantine.rows WHERE run_date = ? AND dataset = ?{cond}", [run_date, dataset, *params])
    new: set = set()
    if qdf is not None and len(qdf):
        out = qdf.assign(run_id=run_id, run_date=run_date, dataset=dataset, reason=qdf["rule_ids"],
                         quarantined_at=datetime.now())[COLUMNS]
        con.register("qdf", out)
        con.execute("INSERT INTO quarantine.rows SELECT * FROM qdf")
        con.unregister("qdf")
        new = {(_clean(k), _clean(o), r)
               for k, o, r in zip(qdf["natural_key"], qdf["order_id"], qdf["rule_ids"], strict=True)}
    changed = old ^ new
    if dataset in ("gateway", "orders"):
        for k, o, _ in changed:
            if k is not None:
                con.execute("INSERT INTO ops.change_log (dataset, key, aux, run_id, changed_at) VALUES (?,?,?,?, now())",
                            [f"quarantine_{dataset}", k, o, run_id])
    return len(changed)
