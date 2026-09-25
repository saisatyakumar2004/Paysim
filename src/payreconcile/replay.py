"""Quarantine replay: re-evaluate quarantined rows against the CURRENT rules and reference data.

Typical use: a merchant was onboarded after its first payments arrived (GW-REF-01), or a rule was corrected.

Outcome per quarantined row (all recorded, append-only, in ops.replay_log):
  RELEASED       passes today: loaded into Silver and removed from quarantine;
  STILL_INVALID  still fails: stays in quarantine with its rule ids refreshed (old and new ids are logged);
  SUPERSEDED     a newer trusted version of the same record is already in Silver: the stale quarantined copy is NOT loaded
                 (it would overwrite newer data); it is kept, re-labelled SUPERSEDED_BY_SILVER, and skipped by later replays.
Never replayed: duplicate-webhook copies (GW-UNQ-01 / ST-UNQ-01), schema-rejected files, superseded rows.

After a release the incremental reconciliation runs (its scope comes from the change log), so Gold is consistent without a
full rebuild; dbt marts are rebuilt when requested. Re-running a replay releases nothing twice.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime

import pandas as pd

from . import db, dq, ingest, quarantine, reconcile, silver, transform

APPLY = {"gateway": silver.apply_gateway, "orders": silver.apply_orders, "settlements": silver.apply_settlements}
SILVER_TABLE = {"gateway": "silver.gateway_txns", "orders": "silver.orders", "settlements": "silver.settlements"}
KEY = {"gateway": "txn_id", "orders": "order_id", "settlements": "settlement_id"}
SUPERSEDED = "SUPERSEDED_BY_SILVER"
SKIP_RULES = ("SCHEMA_REJECT", "GW-UNQ-01", "ST-UNQ-01", SUPERSEDED)


def _superseded_keys(con, dataset: str, good: pd.DataFrame) -> set[str]:
    """Keys of released candidates for which Silver already holds the same or a newer version."""
    if good.empty:
        return set()
    key = KEY[dataset]
    con.register("rp_good", good)
    try:
        if dataset == "orders":
            rows = con.execute("""SELECT trim(g.order_id) FROM rp_good g JOIN silver.orders o ON o.order_id = trim(g.order_id)
                                  WHERE o.updated_at >= pr_ts(g.updated_at)""").fetchall()
        else:
            rows = con.execute(f"SELECT trim(g.{key}) FROM rp_good g JOIN {SILVER_TABLE[dataset]} s "
                               f"ON s.{key} = trim(g.{key})").fetchall()
    finally:
        con.unregister("rp_good")
    return {r[0] for r in rows}


def replay(dataset: str, run_date: date, run_dbt: bool = True) -> dict:
    if dataset not in APPLY:
        raise ValueError(f"replay supports {sorted(APPLY)}")
    replay_id = f"replay-{run_date:%Y%m%d}-{uuid.uuid4().hex[:6]}"
    key = KEY[dataset]
    con = db.connect()
    try:
        placeholders = ",".join("?" * len(SKIP_RULES))
        rows = con.execute(f"SELECT natural_key, order_id, rule_ids, raw_json FROM quarantine.rows "
                           f"WHERE dataset = ? AND run_date = ? AND rule_ids NOT IN ({placeholders})",
                           [dataset, run_date, *SKIP_RULES]).fetchall()
        out = {"run_id": replay_id, "candidates": len(rows), "released": 0, "still_quarantined": 0, "superseded": 0}
        if not rows:
            return out
        df = ingest.to_raw(pd.DataFrame([json.loads(r[3]) for r in rows]))
        res = dq.evaluate(con, dq.load_contract(dataset), df, run_date)
        good = res.good if res.good is not None else df.iloc[0:0]
        stale = _superseded_keys(con, dataset, good)
        fresh = good[~good[key].astype(str).str.strip().isin(stale)] if len(good) else good
        released_keys = {str(k).strip() for k in fresh[key]} if len(fresh) else set()

        if len(fresh):
            APPLY[dataset](con, fresh, replay_id)

        # what stays in quarantine: still-invalid rows (fresh rule ids) + superseded rows (re-labelled, kept for audit)
        keep = res.quarantined if res.quarantined is not None else pd.DataFrame(
            columns=["natural_key", "order_id", "rule_ids", "raw_json"])
        raw_by_key = {str(r[0]).strip(): (r[1], r[3]) for r in rows}
        sup_rows = pd.DataFrame([{"natural_key": k, "order_id": raw_by_key[k][0], "rule_ids": SUPERSEDED,
                                  "raw_json": raw_by_key[k][1]} for k in sorted(stale) if k in raw_by_key])
        new_q = pd.concat([keep, sup_rows], ignore_index=True) if len(sup_rows) else keep
        quarantine.replace(con, replay_id, run_date, dataset, new_q, keep_rules=SKIP_RULES)

        new_rules = {str(k).strip(): r for k, r in zip(keep["natural_key"], keep["rule_ids"], strict=True)}
        now = datetime.now()
        for k, _, old_rules, _ in rows:
            k = str(k).strip()
            outcome = "RELEASED" if k in released_keys else "SUPERSEDED" if k in stale else "STILL_INVALID"
            con.execute("INSERT INTO ops.replay_log VALUES (?,?,?,?,?,?,?,?)", [
                replay_id, run_date, dataset, k, outcome, old_rules,
                new_rules.get(k) if outcome == "STILL_INVALID" else None, now])
        out.update(released=len(released_keys), superseded=len(stale),
                   still_quarantined=len(rows) - len(released_keys) - len(stale))
        con.execute("INSERT INTO ops.task_log (run_id, run_date, task, status, duration_s, rows_in, rows_out, rows_rejected, "
                    "detail) VALUES (?,?,?,?,?,?,?,?,?)",
                    [replay_id, run_date, f"replay_{dataset}", "SUCCESS", 0.0, len(rows), out["released"],
                     out["still_quarantined"], f"superseded={out['superseded']}"])

        latest = con.execute("SELECT max(run_date) FROM ops.run_log WHERE status LIKE 'SUCCESS%'").fetchone()[0]
        as_of = max(run_date, latest) if latest else run_date
        if out["released"] or out["superseded"]:
            st = reconcile.reconcile(con, as_of, replay_id, run_date)          # scope comes from the change log
            out["reconciliation"] = {"mode": st.mode, "items_evaluated": st.items_evaluated, "items_changed": st.items_changed}
    finally:
        con.close()
    if run_dbt and out["released"]:
        d = transform.build(as_of)
        out["dbt"] = {"success": d.success, "passed": d.passed, "failed": d.failed}
    return out
