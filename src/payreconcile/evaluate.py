"""Scores the pipeline against the generator's ground truth (only possible because the data is synthetic).

Four questions:
  1. Did the data-quality layer catch the injected defects (and only those)?
  2. Did reconciliation assign the right status to every transaction?
  3. Were duplicate charges, fuzzy matches and order problems found?
  4. Did the money-flow model find the right stage, cause and amount for every chain (see moneyflow.py / truth.py)?
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
    recon = con.execute("SELECT txn_id, status, match_method, is_duplicate_charge, is_suspected_duplicate, order_issue "
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
    # second tier: SUSPECTED (the original was quarantined, so the pair cannot be confirmed from trusted records)
    suspected = truth["is_suspected_duplicate"].fillna(False).astype(bool)
    report["duplicate_charges_suspected"] = _pr(int((truth_dup & suspected).sum()), int((~truth_dup & suspected).sum()),
                                                int((truth_dup & ~found_dup & ~suspected).sum()))
    either = found_dup | suspected
    report["duplicate_charges_combined"] = _pr(int((truth_dup & either).sum()), int((~truth_dup & either).sum()),
                                               int((truth_dup & ~either).sum()))

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
    trusted_orders = set(con.execute("SELECT DISTINCT order_id FROM silver.gateway_txns WHERE order_id IS NOT NULL "
                                     "AND status = 'captured' AND txn_type = 'payment'").df()["order_id"])
    in_scope = no_order & trusted_orders                 # the payment reached trusted data, so it CAN be flagged
    report["no_order_payments"] = {
        "injected": len(no_order), "flagged": len(set(flagged)),
        "recall": round(len(no_order & set(flagged)) / len(no_order), 4) if no_order else None,
        "trusted_payments": len(in_scope), "flagged_of_trusted": len(in_scope & set(flagged)),
        "recall_on_trusted_payments": round(len(in_scope & set(flagged)) / len(in_scope), 4) if in_scope else None,
        "false_positives": len(set(flagged) - no_order),
        "not_detectable_payment_quarantined": len(no_order - trusted_orders)}

    # headline money
    report["money_at_risk_by_status"] = con.execute(
        "SELECT status, count(*) items, round(sum(money_at_risk), 2) money FROM gold.fact_reconciliation "
        "WHERE money_at_risk > 0 GROUP BY 1 ORDER BY 3 DESC").df().to_dict("records")

    # 4. money flow: stage, cause and amount of every chain ---------------------------------------------------------
    report["money_flow"] = evaluate_money_flow(con, tdir)
    con.close()
    return report


MONEY_TOL = 0.011          # amounts are compared to the cent
NOT_A_DISCREPANCY = ("MATCHED", "PENDING", "TIMING_DELAY", "DATE_ANOMALY")


def evaluate_money_flow(con, tdir) -> dict | None:
    """Score gold.fact_money_flow against truth/money_flow.parquet, chain by chain. Failures are reported, never dropped."""
    path = tdir / "money_flow.parquet"
    if not path.exists():
        return None
    truth = pd.read_parquet(path)
    got = con.execute("SELECT * FROM gold.fact_money_flow").df()
    m = truth.merge(got, on="flow_id", how="outer", suffixes=("_t", "_p"), indicator=True)
    both = m[m["_merge"] == "both"].copy()
    only_truth, only_pipeline = m[m["_merge"] == "left_only"], m[m["_merge"] == "right_only"]

    cls_ok = both["expected_final_status"] == both["classification"]
    bp_ok = both["expected_breakpoint"] == both["first_breakpoint"]
    res_ok = both["expected_resolution"] == both["resolution"]
    t_pos = ~both["expected_final_status"].isin(NOT_A_DISCREPANCY)
    p_pos = ~both["classification"].isin(NOT_A_DISCREPANCY)
    out: dict = {
        "chains": {"truth": int(len(truth)), "pipeline": int(len(got)), "matched_ids": int(len(both)),
                   "truth_only": int(len(only_truth)), "pipeline_only": int(len(only_pipeline))},
        "classification_accuracy": round(float(cls_ok.mean()), 4) if len(both) else None,
        "breakpoint_accuracy": round(float(bp_ok.mean()), 4) if len(both) else None,
        "resolution_accuracy": round(float(res_ok.mean()), 4) if len(both) else None,
        "discrepancy_detection": _pr(int((t_pos & p_pos).sum()), int((~t_pos & p_pos).sum()), int((t_pos & ~p_pos).sum())),
        "root_cause": {"correct": int((t_pos & cls_ok & bp_ok).sum()), "incorrect": int((t_pos & ~(cls_ok & bp_ok)).sum()),
                       "of_truth_discrepancies": int(t_pos.sum())},
        "unexplained": {"classified_unexplained": int((both["classification"] == "UNEXPLAINED").sum()),
                        "truth_unexplained": int((both["expected_final_status"] == "UNEXPLAINED").sum()),
                        "pipeline_open_resolution": int((both["resolution"] == "OPEN").sum()),
                        "pipeline_blocked_resolution": int((both["resolution"] == "BLOCKED").sum())},
    }
    per = {}
    for c in sorted(set(both["expected_final_status"]) | set(both["classification"])):
        per[c] = _pr(int(((both["expected_final_status"] == c) & (both["classification"] == c)).sum()),
                     int(((both["expected_final_status"] != c) & (both["classification"] == c)).sum()),
                     int(((both["expected_final_status"] == c) & (both["classification"] != c)).sum()))
    out["per_classification"] = per
    conf = (both[~cls_ok].groupby(["expected_final_status", "classification"]).size().reset_index(name="n")
            .sort_values("n", ascending=False))
    out["misclassified"] = conf.to_dict("records")

    # amount agreement on chains whose gateway record is visible (a quarantined record has no verified gateway amount)
    visible = both[both["flow_type_t"].isin(["PAYMENT", "REFUND"]) & both["flow_type_p"].isin(["PAYMENT", "REFUND"])]
    fields = {"customer_amount": ("expected_customer_amount", "order_amount"),
              "gateway_amount": ("expected_gateway_amount", "gateway_gross"),
              "expected_fee": ("expected_fee_t", "expected_fee_p"),
              "expected_net": ("expected_net_settlement", "expected_net"),
              "bank_settlement": ("expected_bank_settlement", "bank_net")}
    checks = {}
    for name, (tc, pc) in fields.items():
        a, b = visible[tc].astype(float), visible[pc].astype(float)
        agree = (a.isna() & b.isna()) | ((a - b).abs() <= MONEY_TOL)
        checks[name] = {"compared": int(len(visible)), "differ": int((~agree).sum())}
    out["field_agreement"] = checks

    te, pe = both["expected_exposure"].fillna(0.0), both["exposure_amount"].fillna(0.0)
    tot_t = float(truth["expected_exposure"].fillna(0).sum())
    diff = float((te - pe).abs().sum() + only_truth["expected_exposure"].fillna(0).sum()
                 + only_pipeline["exposure_amount"].fillna(0).sum())
    out["money"] = {"truth_exposure": round(tot_t, 2), "pipeline_exposure": round(float(got["exposure_amount"].fillna(0).sum()), 2),
                    "absolute_difference": round(diff, 2),
                    "difference_pct_of_truth": round(100 * diff / tot_t, 4) if tot_t else None,
                    "truth_exposure_by_resolution": truth.groupby("expected_resolution")["expected_exposure"].sum().round(2).to_dict(),
                    "pipeline_exposure_by_resolution": got.groupby("resolution")["exposure_amount"].sum().round(2).to_dict()}
    cols = ["flow_id", "expected_final_status", "classification", "expected_breakpoint", "first_breakpoint",
            "expected_exposure", "exposure_amount"]
    out["worst_misses"] = (both[~(cls_ok & bp_ok)].assign(_gap=(te - pe).abs()).sort_values("_gap", ascending=False)
                           .head(10)[cols].to_dict("records"))
    return out


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
        line("duplicate charges (confirmed)", r["duplicate_charges"])
        line("duplicate charges (suspected)", r["duplicate_charges_suspected"])
        line("duplicate charges (combined)", r["duplicate_charges_combined"])
    line("orphan settlements", r["orphan_settlements"])
    line("settlements of quarantined txns", r["source_quarantined_settlements"])
    print(f"  order without payment: {r['order_without_payment']}")
    print(f"  payment without order: {r['no_order_payments']}")
    print("\nMONEY AT RISK, engine view (INR, gross basis)")
    for m in r["money_at_risk_by_status"]:
        print(f"  {m['status']:<22} items={m['items']:<6} {m['money']:>14,.2f}")
    mf = r.get("money_flow")
    if mf:
        print("\nMONEY FLOW (chain by chain against the injected financial truth)")
        print(f"  chains {mf['chains']}")
        print(f"  classification accuracy={mf['classification_accuracy']}  breakpoint accuracy={mf['breakpoint_accuracy']}  "
              f"resolution accuracy={mf['resolution_accuracy']}")
        line("discrepancy detection", mf["discrepancy_detection"])
        print(f"  root cause: {mf['root_cause']}")
        print(f"  unexplained: {mf['unexplained']}")
        for c, d in mf["per_classification"].items():
            line(c, d)
        print(f"  field agreement, chains that differ: {dict((k, v['differ']) for k, v in mf['field_agreement'].items())}")
        print(f"  money: {mf['money']}")
        if mf["misclassified"]:
            print("  misclassified (truth -> pipeline):")
            for x in mf["misclassified"][:12]:
                print(f"    {x['expected_final_status']:<26} -> {x['classification']:<26} n={x['n']}")
