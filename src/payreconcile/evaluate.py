"""Scores the pipeline against the generator's ground truth (only possible because the data is synthetic).

Three questions:
  1. Did the data-quality layer catch the injected defects (and only those)?
  2. Did reconciliation assign the right status to every transaction?
  3. Were duplicate charges, fuzzy matches and order problems found?
"""

from __future__ import annotations

import duckdb
import pandas as pd

from . import config


def _pr(tp: int, fp: int, fn: int) -> dict:
    return {"tp": tp, "fp": fp, "fn": fn,
            "precision": round(tp / (tp + fp), 4) if tp + fp else None,
            "recall": round(tp / (tp + fn), 4) if tp + fn else None}


def evaluate_all(db_path=None, truth_path=None) -> dict:
    con = duckdb.connect(str(db_path or config.warehouse_path()), read_only=True)
    tdir = truth_path or config.truth_dir()
    t_txn = pd.read_parquet(tdir / "txn.parquet")
    t_stl = pd.read_parquet(tdir / "settlement.parquet")
    t_dq = pd.read_parquet(tdir / "dq.parquet")
    t_dq = t_dq[t_dq["dataset"] != ""]
    t_ord = pd.read_parquet(tdir / "orders.parquet")
    t_ord = t_ord[t_ord["order_id"] != ""]
    report: dict = {}

    # 1. data quality -----------------------------------------------------------------------------
    quarantined = con.execute("SELECT DISTINCT dataset, natural_key FROM quarantine.rows "
                              "WHERE natural_key IS NOT NULL").df()
    q_set = set(map(tuple, quarantined.to_numpy()))
    truth_q = t_dq[t_dq["action"] == "quarantine"]
    tq_set = set(zip(truth_q["dataset"], truth_q["key"], strict=True))
    # null_order_id rows are keyed by txn_id too, so all quarantine defects are comparable on (dataset, key)
    tp = len(q_set & tq_set)
    report["dq_quarantine"] = _pr(tp, len(q_set - tq_set), len(tq_set - q_set))
    by_defect = {}
    for defect, grp in truth_q.groupby("defect"):
        keys = set(zip(grp["dataset"], grp["key"], strict=True))
        by_defect[defect] = {"injected": len(keys), "caught": len(keys & q_set),
                             "recall": round(len(keys & q_set) / len(keys), 4)}
    report["dq_recall_by_defect"] = by_defect
    repaired = con.execute("SELECT rule_id, sum(violations) FROM ops.dq_results WHERE rule_id LIKE '%FIX%' "
                           "AND run_id IN (SELECT arg_max(run_id, started_at) FROM ops.run_log GROUP BY run_date) "
                           "GROUP BY 1").fetchall()
    inj = t_dq[t_dq["action"] == "repair"]["defect"].value_counts().to_dict()
    report["dq_repairs"] = {"reported_by_rules": dict(repaired), "injected": inj}

    # 2. reconciliation status ------------------------------------------------------------------------
    recon = con.execute("SELECT txn_id, status, match_method, is_duplicate_charge, order_issue "
                        "FROM gold.fact_reconciliation WHERE item_type = 'txn'").df()
    truth = t_txn[t_txn["expected_status"] != "QUARANTINED"].merge(recon, on="txn_id", how="left")
    report["recon_txn_population"] = {"truth_txns": int(len(truth)), "found_in_gold": int(truth["status"].notna().sum())}
    correct = (truth["expected_status"] == truth["status"])
    report["recon_status_accuracy"] = round(float(correct.mean()), 4)
    per_status = {}
    for s in sorted(set(truth["expected_status"])):
        tp_ = int(((truth["expected_status"] == s) & (truth["status"] == s)).sum())
        fn_ = int(((truth["expected_status"] == s) & (truth["status"] != s)).sum())
        fp_ = int(((truth["expected_status"] != s) & (truth["status"] == s)).sum())
        per_status[s] = _pr(tp_, fp_, fn_)
    report["recon_per_status"] = per_status
    report["recon_confusion"] = (truth.groupby(["expected_status", "status"], dropna=False).size()
                                 .reset_index(name="n").to_dict("records"))

    # fuzzy matching
    fz_tp = int((truth["expected_fuzzy"] & (truth["match_method"] == "fuzzy")).sum())
    fz_fp = int((~truth["expected_fuzzy"] & (truth["match_method"] == "fuzzy")).sum())
    fz_fn = int((truth["expected_fuzzy"] & (truth["match_method"] != "fuzzy")).sum())
    report["fuzzy_match"] = _pr(fz_tp, fz_fp, fz_fn)

    # duplicate charges
    truth_dup = truth["is_duplicate_charge_x"].astype(bool)
    found_dup = truth["is_duplicate_charge_y"].fillna(False).astype(bool)
    report["duplicate_charges"] = _pr(int((truth_dup & found_dup).sum()), int((~truth_dup & found_dup).sum()),
                                      int((truth_dup & ~found_dup).sum()))

    # 3. settlement-side items ----------------------------------------------------------------------------
    stl_items = con.execute("SELECT settlement_id, status FROM gold.fact_reconciliation "
                            "WHERE item_type = 'settlement'").df()
    orphans_truth = set(t_stl[t_stl["kind"] == "orphan"]["settlement_id"])
    orphans_found = set(stl_items[stl_items["status"] == "ORPHAN_SETTLEMENT"]["settlement_id"])
    report["orphan_settlements"] = _pr(len(orphans_truth & orphans_found), len(orphans_found - orphans_truth),
                                       len(orphans_truth - orphans_found))
    sq_truth = set(t_stl[t_stl["expected_status"] == "SOURCE_QUARANTINED"]["settlement_id"])
    sq_found = set(stl_items[stl_items["status"] == "SOURCE_QUARANTINED"]["settlement_id"])
    report["source_quarantined_settlements"] = _pr(len(sq_truth & sq_found), len(sq_found - sq_truth),
                                                   len(sq_truth - sq_found))

    # order-side items (recall only: some flags legitimately arise from quarantined rows)
    no_pay = set(t_ord[t_ord["issue"] == "ORDER_WITHOUT_PAYMENT"]["order_id"])
    found_orders = con.execute("SELECT order_id FROM gold.fact_reconciliation WHERE status = 'ORDER_WITHOUT_PAYMENT'"
                               ).df()["order_id"]
    eligible = set(con.execute("SELECT order_id FROM silver.orders WHERE cast(created_at AS DATE) + 2 < "
                               "(SELECT as_of_date FROM ops.recon_state WHERE id = 1)").df()["order_id"])
    no_pay_elig = no_pay & eligible
    report["order_without_payment"] = {"injected_eligible": len(no_pay_elig),
                                       "caught": len(no_pay_elig & set(found_orders)),
                                       "recall": round(len(no_pay_elig & set(found_orders)) / len(no_pay_elig), 4)
                                       if no_pay_elig else None}
    no_order = set(t_ord[t_ord["issue"] == "NO_ORDER"]["order_id"])
    flagged = con.execute("SELECT r.order_id FROM gold.fact_reconciliation r WHERE r.order_issue = 'NO_ORDER'"
                          ).df()["order_id"]
    report["no_order_payments"] = {"injected": len(no_order), "flagged": len(set(flagged)),
                                   "recall": round(len(no_order & set(flagged)) / len(no_order), 4) if no_order else None}

    # headline money
    report["money_at_risk_by_status"] = con.execute(
        "SELECT status, count(*) items, round(sum(money_at_risk), 2) money FROM gold.fact_reconciliation "
        "WHERE money_at_risk > 0 GROUP BY 1 ORDER BY 3 DESC").df().to_dict("records")
    con.close()
    return report


def print_report(r: dict) -> None:
    def line(name, d):
        print(f"  {name:<34} precision={d['precision']}  recall={d['recall']}  (tp={d['tp']} fp={d['fp']} fn={d['fn']})")

    print("\nDATA QUALITY (quarantine vs injected defects)")
    line("quarantine (all datasets)", r["dq_quarantine"])
    for k, v in r["dq_recall_by_defect"].items():
        print(f"    {k:<18} injected={v['injected']:<5} caught={v['caught']:<5} recall={v['recall']}")
    print(f"  repairs reported {r['dq_repairs']['reported_by_rules']}  vs injected {r['dq_repairs']['injected']}")
    print("\nRECONCILIATION")
    print(f"  txn population {r['recon_txn_population']}  status accuracy = {r['recon_status_accuracy']}")
    for s, d in r["recon_per_status"].items():
        line(s, d)
    line("fuzzy matches", r["fuzzy_match"])
    if "duplicate_charges" in r:
        line("duplicate charges", r["duplicate_charges"])
    line("orphan settlements", r["orphan_settlements"])
    line("settlements of quarantined txns", r["source_quarantined_settlements"])
    print(f"  order without payment: {r['order_without_payment']}")
    print(f"  payment without order: {r['no_order_payments']}")
    print("\nMONEY AT RISK (INR)")
    for m in r["money_at_risk_by_status"]:
        print(f"  {m['status']:<22} items={m['items']:<6} {m['money']:>14,.2f}")
