"""Quarantine replay: re-evaluate quarantined rows against the CURRENT rules and reference data.

Typical use: a merchant was onboarded after its first payments arrived (GW-REF-01), or a rule was corrected. Rows that now pass
are loaded to Silver and leave quarantine; rows that still fail stay, with their rule ids refreshed. Duplicate-only rows
(the second copy of a key) and schema-rejected files are not replayed.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime

import pandas as pd

from . import db, dq, ingest, silver, transform

APPLY = {"gateway": silver.apply_gateway, "orders": silver.apply_orders, "settlements": silver.apply_settlements}
SKIP_RULES = ("SCHEMA_REJECT", "GW-UNQ-01", "ST-UNQ-01")


def replay(dataset: str, run_date: date, run_dbt: bool = True) -> dict:
    if dataset not in APPLY:
        raise ValueError(f"replay supports {sorted(APPLY)}")
    run_id = f"replay-{run_date:%Y%m%d}-{uuid.uuid4().hex[:6]}"
    con = db.connect()
    try:
        rows = con.execute("SELECT natural_key, rule_ids, raw_json FROM quarantine.rows "
                           "WHERE dataset = ? AND run_date = ? AND rule_ids NOT IN (?, ?, ?)",
                           [dataset, run_date, *SKIP_RULES]).fetchall()
        if not rows:
            return {"run_id": run_id, "candidates": 0, "released": 0, "still_quarantined": 0}
        df = ingest.to_raw(pd.DataFrame([json.loads(r[2]) for r in rows]))
        res = dq.evaluate(con, dq.load_contract(dataset), df, run_date)
        released = 0 if res.good is None else len(res.good)
        if released:
            APPLY[dataset](con, res.good, run_id)
        # replace this day's replayable quarantine rows with whatever still fails
        con.execute("DELETE FROM quarantine.rows WHERE dataset = ? AND run_date = ? AND rule_ids NOT IN (?, ?, ?)",
                    [dataset, run_date, *SKIP_RULES])
        q = res.quarantined
        if q is not None and len(q):
            qdf = q.assign(run_id=run_id, run_date=run_date, dataset=dataset, reason=q["rule_ids"],
                           quarantined_at=datetime.now())
            qdf = qdf[["run_id", "run_date", "dataset", "natural_key", "order_id", "rule_ids", "reason", "raw_json",
                       "quarantined_at"]]
            con.register("qdf", qdf)
            con.execute("INSERT INTO quarantine.rows SELECT * FROM qdf")
            con.unregister("qdf")
        con.execute("INSERT INTO ops.task_log VALUES (?,?,?,?,?,?,?,?,?)", [
            run_id, run_date, f"replay_{dataset}", "SUCCESS", 0.0, len(rows), released, len(rows) - released, None])
        latest = con.execute("SELECT max(run_date) FROM ops.run_log WHERE status LIKE 'SUCCESS%'").fetchone()[0]
    finally:
        con.close()
    out = {"run_id": run_id, "candidates": len(rows), "released": released, "still_quarantined": len(rows) - released}
    if run_dbt and released:
        d = transform.build(max(run_date, latest) if latest else run_date)
        out["dbt"] = {"success": d.success, "passed": d.passed, "failed": d.failed}
    return out
