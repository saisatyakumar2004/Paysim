"""Root-cause analysis of the reconciliation misses, plus a precision experiment on candidate detection rules.

Reads the warehouse and the generator's ground truth (read-only) and writes docs/results_miss_analysis.json.
Nothing here changes pipeline behaviour: it exists so the conclusions in docs/investigation_misses.md are reproducible.

Usage:  python scripts/analyze_misses.py [--out docs/results_miss_analysis.json]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from payreconcile import config, db  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "docs" / "results_miss_analysis.json"))
    args = ap.parse_args()

    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    con.execute("CREATE TEMP MACRO pr_amt(x) AS try_cast(replace(replace(replace(trim(x), '₹', ''), ',', ''), ' ', '') AS DOUBLE)")
    con.execute("""CREATE TEMP MACRO pr_ts(x) AS coalesce(try_strptime(trim(x), '%Y-%m-%dT%H:%M:%S'),
                   try_strptime(trim(x), '%d/%m/%Y %H:%M:%S'), try_strptime(trim(x), '%Y-%m-%d %H:%M:%S'))""")
    truth = pd.read_parquet(config.truth_dir() / "txn.parquet")
    t_ord = pd.read_parquet(config.truth_dir() / "orders.parquet")
    t_ord = t_ord[t_ord["order_id"] != ""]
    out: dict = {"warehouse": str(config.warehouse_path())}

    # ---- A. duplicate-charge misses ---------------------------------------------------------------------------------
    fact = con.execute("SELECT txn_id, order_id, is_duplicate_charge AS flag FROM gold.fact_reconciliation "
                       "WHERE item_type = 'txn'").df()
    dup = truth[truth["is_duplicate_charge"]].merge(fact, on="txn_id", how="left")
    missed = dup[~dup["flag"].fillna(False).astype(bool)]
    cats: Counter = Counter()
    detail = []
    for _, r in missed.iterrows():
        q_by_order = con.execute("SELECT natural_key, rule_ids FROM quarantine.rows WHERE dataset = 'gateway' "
                                 "AND order_id = ? AND natural_key <> ?", [r["order_id"], r["txn_id"]]).fetchall()
        if q_by_order:
            cat = "original_quarantined_traceable_by_order_id"
        else:
            # the original may have been quarantined WITHOUT an order id (GW-CMP-02): look for a null-order quarantined
            # payload with the same merchant + amount within 10 minutes
            t = con.execute("SELECT merchant_id, amount, created_at FROM silver.gateway_txns WHERE txn_id = ?",
                            [r["txn_id"]]).fetchone()
            cand = con.execute("""SELECT natural_key FROM quarantine.rows WHERE dataset = 'gateway' AND order_id IS NULL
                                  AND json_extract_string(raw_json, '$.merchant_id') = ?
                                  AND pr_amt(json_extract_string(raw_json, '$.amount')) = ?
                                  AND abs(epoch(pr_ts(json_extract_string(raw_json, '$.created_at')) - ?)) <= 600""",
                                [t[0], t[1], t[2]]).fetchall()
            cat = "original_quarantined_without_order_id" if cand else "unexplained"
        cats[cat] += 1
        detail.append({"txn_id": r["txn_id"], "order_id": r["order_id"], "category": cat,
                       "quarantine_rules_on_order": [x[1] for x in q_by_order]})
    n_truth = len(dup)
    out["duplicate_charge"] = {
        "truth_duplicates": n_truth, "missed": len(missed), "categories": dict(cats),
        "recall_overall": round(1 - len(missed) / n_truth, 4),
        "recall_when_original_is_trusted": round(1.0 - (len(missed) - sum(cats.values())) / (n_truth - sum(cats.values())), 4),
        "missed_detail": detail,
    }

    # ---- B. candidate rule 1: order-keyed evidence from a quarantined original (safe, order_id-anchored) -----------
    r1 = con.execute("""
        SELECT DISTINCT t.txn_id FROM silver.gateway_txns t
        JOIN quarantine.rows q ON q.dataset = 'gateway' AND q.order_id = t.order_id AND q.natural_key <> t.txn_id
        WHERE t.status = 'captured' AND t.txn_type = 'payment'""").df()
    flagged1 = set(r1["txn_id"])
    truth_dups = set(truth.loc[truth["is_duplicate_charge"], "txn_id"])
    already = set(fact.loc[fact["flag"].fillna(False).astype(bool), "txn_id"])
    new1 = flagged1 - already
    out["candidate_rule_order_keyed"] = {
        "description": "captured payment whose ORDER also has a quarantined gateway row with a different txn_id",
        "newly_flagged": len(new1), "true_duplicates_among_new": len(new1 & truth_dups),
        "false_positives_among_new": len(new1 - truth_dups),
        "precision_of_new_flags": round(len(new1 & truth_dups) / len(new1), 4) if new1 else None}

    # ---- C. candidate rule 2: unanchored (merchant + amount + time), the only option for a null order id ------------
    r2 = con.execute("""
        SELECT DISTINCT t.txn_id FROM silver.gateway_txns t
        JOIN quarantine.rows q ON q.dataset = 'gateway' AND q.order_id IS NULL
         AND json_extract_string(q.raw_json, '$.merchant_id') = t.merchant_id
         AND pr_amt(json_extract_string(q.raw_json, '$.amount')) = t.amount
         AND abs(epoch(pr_ts(json_extract_string(q.raw_json, '$.created_at')) - t.created_at)) <= 600
        WHERE t.status = 'captured' AND t.txn_type = 'payment'""").df()
    flagged2 = set(r2["txn_id"]) - already
    out["candidate_rule_unanchored"] = {
        "description": "captured payment with a NULL-order-id quarantined row of the same merchant, amount and time (<=10 min)",
        "newly_flagged": len(flagged2), "true_duplicates_among_new": len(flagged2 & truth_dups),
        "false_positives_among_new": len(flagged2 - truth_dups),
        "precision_of_new_flags": round(len(flagged2 & truth_dups) / len(flagged2), 4) if flagged2 else None}

    # ---- D. payment-without-order misses ------------------------------------------------------------------------------
    no_order = set(t_ord.loc[t_ord["issue"] == "NO_ORDER", "order_id"])
    flagged = set(con.execute("SELECT order_id FROM gold.fact_reconciliation WHERE order_issue = 'NO_ORDER'")
                  .df()["order_id"])
    miss_orders = sorted(no_order - flagged)
    rules: Counter = Counter()
    in_silver = 0
    for oid in miss_orders:
        s = con.execute("SELECT count(*) FROM silver.gateway_txns WHERE order_id = ?", [oid]).fetchone()[0]
        in_silver += s > 0
        for (rid,) in con.execute("SELECT rule_ids FROM quarantine.rows WHERE dataset = 'gateway' AND order_id = ?",
                                  [oid]).fetchall():
            rules[rid] += 1
    out["payment_without_order"] = {
        "truth": len(no_order), "flagged": len(no_order & flagged), "missed": len(miss_orders),
        "false_positives": len(flagged - no_order),
        "missed_with_a_trusted_payment_in_silver": int(in_silver),
        "missed_payments_were_quarantined_by": dict(rules),
        "recall_overall": round(len(no_order & flagged) / len(no_order), 4),
        "recall_on_trusted_payments": round(len(no_order & flagged) / (len(no_order & flagged) + in_silver), 4),
    }
    con.close()
    Path(args.out).write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: v for k, v in out.items() if k != "duplicate_charge"} | {
        "duplicate_charge": {k: v for k, v in out["duplicate_charge"].items() if k != "missed_detail"}}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
