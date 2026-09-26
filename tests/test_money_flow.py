"""Money-flow lineage: one hand-computed scenario per way money can stop matching.

Every scenario pushes rows through the real Silver upserts, the reconciliation engine and `moneyflow.build`. Amounts are
worked out by hand: an order of INR 1,000 at merchant M1 (fee 2.00 %) must produce a scheduled fee of 20 and an expected
settlement of 980; the assertions state those numbers, not what the implementation happens to return.
"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd

from payreconcile import moneyflow, quarantine, reconcile, silver

AS_OF = "2026-01-10"
_runs = iter(range(1, 10_000))


def gw(txn_id, order_id="O1", merchant="M1", amount="1000", created="2026-01-01T10:00:00", txn_type="payment",
       refund_of=None, status="captured"):
    return {"txn_id": txn_id, "order_id": order_id, "merchant_id": merchant, "amount": amount, "currency": "INR",
            "status": status, "method": "upi", "txn_type": txn_type, "refund_of": refund_of, "created_at": created}


def bank(sid, ref, gross="1000", fee="20", net=None, merchant="M1", on="2026-01-02"):
    net = net if net is not None else str(round(float(gross) - float(fee), 2))
    return {"settlement_id": sid, "gateway_ref": ref, "merchant_id": merchant, "gross_amount": gross, "fee_amount": fee,
            "net_amount": net, "settlement_date": on}


def order(oid="O1", amount="1000", status="paid", created="2026-01-01 09:00:00", merchant="M1"):
    return {"order_id": oid, "merchant_id": merchant, "customer_id": "C1", "order_amount": amount, "order_status": status,
            "created_at": created, "updated_at": created}


def run(con, txns=(), stls=(), orders=(), as_of=AS_OF):
    """Silver upserts -> reconciliation engine -> money flow. Returns nothing; read chains with `flow`."""
    r = f"run{next(_runs)}"
    if txns:
        silver.apply_gateway(con, pd.DataFrame(list(txns)).astype(object), r)
    if stls:
        silver.apply_settlements(con, pd.DataFrame(list(stls)).astype(object), r)
    if orders:
        silver.apply_orders(con, pd.DataFrame(list(orders)).astype(object), r)
    reconcile.reconcile(con, date.fromisoformat(as_of), r, date.fromisoformat(as_of))
    moneyflow.build(con, r)


def flow(con, flow_id):
    cur = con.execute("SELECT * FROM gold.fact_money_flow WHERE flow_id = ?", [flow_id])
    cols = [c[0] for c in cur.description]
    row = cur.fetchone()
    assert row is not None, f"no chain {flow_id}: {con.execute('SELECT flow_id FROM gold.fact_money_flow').fetchall()}"
    return dict(zip(cols, row, strict=True))


def total_exposure(con):
    return round(con.execute("SELECT coalesce(sum(exposure_amount), 0) FROM gold.fact_money_flow").fetchone()[0], 2)


def approx(a, b, tol=0.011):
    return a is not None and abs(a - b) <= tol


# ---- 1. perfect match -------------------------------------------------------------------------------------------------
def test_a_perfect_chain_conserves_every_rupee_and_stops_at_the_bank(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1")], [order()])
    f = flow(con, "TXN:T1")
    assert (f["order_amount"], f["gateway_gross"], f["expected_fee"], f["expected_net"], f["bank_net"]) == (1000, 1000, 20, 980, 980)
    assert f["classification"] == "MATCHED" and f["resolution"] == "RECONCILED" and f["first_breakpoint"] == "NONE"
    assert (f["gap_gateway"], f["gap_bank_amount"], f["gap_fee"], f["gap_bank_net"], f["difference_amount"]) == (0, 0, 0, 0, 0)
    assert f["exposure_amount"] == 0 and f["evidence_level"] == "HIGH" and f["match_method"] == "exact"
    assert f["merchant_amount"] is None and f["merchant_stage"] == "NOT_AVAILABLE"        # never fabricated
    assert moneyflow.check_invariants(con)["ok"]


# ---- 2. gateway mismatch ------------------------------------------------------------------------------------------------
def test_gateway_amount_below_the_order_breaks_at_the_gateway_not_the_bank(con):
    run(con, [gw("T1", amount="950")], [bank("S1", "GW-T1", gross="950", fee="19", net="931")], [order(amount="1000")])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "GATEWAY_AMOUNT_MISMATCH" and f["first_breakpoint"] == "GATEWAY" and f["resolution"] == "OPEN"
    assert f["gap_gateway"] == -50 and f["difference_amount"] == 0                     # the bank agrees with the gateway
    assert f["expected_net"] == 931 and f["bank_net"] == 931 and f["exposure_amount"] == 50


# ---- 3. bank mismatch --------------------------------------------------------------------------------------------------
def test_bank_paying_less_than_captured_breaks_at_the_bank_settlement(con):
    # gross 970 at 2 % = fee 19.40, net 950.60; the gateway captured 1000 so 980 should have settled
    run(con, [gw("T1")], [bank("S1", "GW-T1", gross="970", fee="19.4", net="950.6")], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "BANK_AMOUNT_MISMATCH" and f["first_breakpoint"] == "BANK_SETTLEMENT"
    assert f["resolution"] == "OPEN" and f["gap_gateway"] == 0
    assert approx(f["difference_amount"], -29.40) and approx(f["gap_bank_amount"], -29.40)
    assert f["gap_fee"] == 0 and f["gap_bank_net"] == 0                                # not a fee or arithmetic problem
    assert approx(f["exposure_amount"], 29.40) and f["root_cause"] == "BANK_PAID_LESS_THAN_CAPTURED"


# ---- 4. merchant settlement -----------------------------------------------------------------------------------------------
def test_merchant_stage_is_never_fabricated_but_a_wrong_credit_is_caught(con):
    run(con, [gw("T1"), gw("T2", order_id="O2")],
        [bank("S1", "GW-T1"), bank("S2", "GW-T2", merchant="M2")], [order(), order("O2")])
    right, wrong = flow(con, "TXN:T1"), flow(con, "TXN:T2")
    assert right["classification"] == "MATCHED"
    assert wrong["classification"] == "MERCHANT_SETTLEMENT_MISMATCH" and wrong["first_breakpoint"] == "MERCHANT_SETTLEMENT"
    assert wrong["exposure_amount"] == 980                                             # the money went to the wrong merchant
    assert con.execute("SELECT count(*) FROM gold.fact_money_flow WHERE merchant_amount IS NOT NULL").fetchone()[0] == 0
    stages = moneyflow.investigate(con, "TXN:T1")["stages"]
    assert stages[-1]["stage"] == "MERCHANT_SETTLEMENT" and stages[-1]["amount"] is None
    assert "NOT AVAILABLE" in stages[-1]["note"]


# ---- 5. missing settlement -----------------------------------------------------------------------------------------------
def test_missing_settlement_is_called_missing_only_after_every_check_finds_nothing(con):
    run(con, [gw("T1")], [], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "MISSING_SETTLEMENT" and f["first_breakpoint"] == "BANK_SETTLEMENT" and f["resolution"] == "OPEN"
    assert f["expected_net"] == 980 and f["bank_net"] is None and f["difference_amount"] == -980
    assert f["exposure_amount"] == 980 and f["root_cause"] == "NO_BANK_RECORD_AFTER_ALL_EVIDENCE_CHECKS"


def test_a_settlement_that_is_not_due_yet_is_in_flight_not_missing(con):
    run(con, [gw("T1")], [], [order()], as_of="2026-01-03")
    f = flow(con, "TXN:T1")
    assert f["classification"] == "PENDING" and f["resolution"] == "IN_FLIGHT" and f["exposure_amount"] == 0
    assert f["difference_amount"] == -980                                              # the gap exists, it is just not due


# ---- 6. delayed settlement / 19. wrong date --------------------------------------------------------------------------------
def test_a_settlement_that_arrives_after_the_sla_is_a_timing_finding_with_no_money_at_risk(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1", on="2026-01-08")], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "TIMING_DELAY" and f["resolution"] == "TIMING" and f["settlement_lag_days"] == 7
    assert f["difference_amount"] == 0 and f["exposure_amount"] == 0


def test_a_settlement_dated_before_the_capture_is_an_anomaly_not_a_missing_payment(con):
    run(con, [gw("T1", created="2026-01-05T10:00:00")], [bank("S1", "GW-T1", on="2026-01-03")],
        [order(created="2026-01-05 09:00:00")])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "DATE_ANOMALY" and f["settlement_lag_days"] == -2 and f["exposure_amount"] == 0


# ---- 7. duplicate settlement / 15. multiple / 16. split ----------------------------------------------------------------
def test_the_same_reference_settled_twice_is_a_duplicate_settlement_of_980(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1"), bank("S2", "GW-T1", on="2026-01-03")], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "DUPLICATE_SETTLEMENT" and f["first_breakpoint"] == "BANK_SETTLEMENT"
    assert f["resolution"] == "EXPLAINED" and f["settlement_pattern"] == "DUPLICATE" and f["n_settlements"] == 2
    assert f["bank_net"] == 1960 and approx(f["difference_amount"], 980) and approx(f["exposure_amount"], 980)
    assert con.execute("SELECT count(*) FROM gold.fact_reconciliation WHERE item_id = 'TXN:T1'").fetchone()[0] == 1


def test_two_bank_records_that_are_neither_a_split_nor_a_duplicate_are_a_mismatch(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1", gross="700", fee="14"), bank("S2", "GW-T1", gross="700", fee="14")], [order()])
    f = flow(con, "TXN:T1")
    assert f["settlement_pattern"] == "MULTIPLE" and f["classification"] == "BANK_AMOUNT_MISMATCH"
    assert approx(f["gap_bank_amount"], 1372 - 980)                                    # 1400 gross - 28 fee vs 980


def test_a_settlement_split_over_two_records_conserves_the_money(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1", gross="600", fee="12"), bank("S2", "GW-T1", gross="400", fee="8")], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "MATCHED" and f["settlement_pattern"] == "SPLIT" and f["n_settlements"] == 2
    assert f["bank_gross"] == 1000 and f["bank_fee"] == 20 and f["bank_net"] == 980 and f["exposure_amount"] == 0
    assert f["settlement_ids"] == "S1,S2"


# ---- 8. duplicate payment -------------------------------------------------------------------------------------------------
def test_a_second_capture_of_one_order_is_a_duplicate_payment_and_the_order_is_counted_once(con):
    run(con, [gw("T1"), gw("T2", created="2026-01-01T10:03:00")],
        [bank("S1", "GW-T1"), bank("S2", "GW-T2")], [order()])
    first, second = flow(con, "TXN:T1"), flow(con, "TXN:T2")
    assert first["classification"] == "MATCHED" and first["order_amount"] == 1000
    assert second["classification"] == "DUPLICATE_PAYMENT" and second["first_breakpoint"] == "GATEWAY"
    assert second["resolution"] == "EXPLAINED" and second["order_amount"] is None
    assert second["gap_gateway"] == 1000 and second["exposure_amount"] == 1000       # the customer was charged twice
    assert con.execute("SELECT sum(order_amount) FROM gold.fact_money_flow").fetchone()[0] == 1000


def test_a_second_capture_whose_original_was_quarantined_is_a_suspected_duplicate_and_the_order_is_still_counted_once(con):
    quarantine_gateway(con, "T1", status="captred")
    run(con, [gw("T2", created="2026-01-01T10:03:00")], [bank("S1", "GW-T1"), bank("S2", "GW-T2")], [order()])
    original, second = flow(con, "QTX:T1"), flow(con, "TXN:T2")
    assert original["order_amount"] == 1000 and original["classification"] == "QUARANTINED_SOURCE"
    assert second["classification"] == "DUPLICATE_PAYMENT" and second["resolution"] == "BLOCKED"      # suspected, not confirmed
    assert second["order_amount"] is None and second["gap_gateway"] == 1000 and second["evidence_level"] == "LOW"
    assert con.execute("SELECT sum(order_amount) FROM gold.fact_money_flow").fetchone()[0] == 1000


# ---- 9. fee mismatch ----------------------------------------------------------------------------------------------------
def test_a_fee_above_the_schedule_breaks_at_processing_and_leaves_the_gross_alone(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1", gross="1000", fee="50", net="950")], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "FEE_MISMATCH" and f["first_breakpoint"] == "PROCESSING" and f["resolution"] == "OPEN"
    assert f["gap_fee"] == -30 and f["gap_bank_amount"] == 0 and f["difference_amount"] == -30
    assert f["expected_fee"] == 20 and f["bank_fee"] == 50 and f["exposure_amount"] == 30


def test_a_fee_row_that_failed_validation_blocks_the_fee_conclusion(con):
    quarantine.replace(con, "r", date(2026, 1, 1), "merchants", pd.DataFrame(
        [{"natural_key": "M1", "order_id": None, "rule_ids": "MR-VAL-01", "raw_json": json.dumps({"merchant_id": "M1", "fee_pct": "25"})}]))
    run(con, [gw("T1")], [bank("S1", "GW-T1", gross="1000", fee="50", net="950")], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "INSUFFICIENT_EVIDENCE" and f["first_breakpoint"] == "SOURCE_DATA_QUALITY"
    assert f["resolution"] == "BLOCKED" and f["root_cause"] == "FEE_SCHEDULE_ROW_QUARANTINED"


# ---- 10. refunds --------------------------------------------------------------------------------------------------------
def refund(rid, amount, of="T1", created="2026-01-03T10:00:00"):
    return gw(rid, txn_type="refund", refund_of=of, amount=amount, created=created)


def test_a_refund_larger_than_the_payment_is_a_refund_mismatch_of_the_excess(con):
    run(con, [gw("T1"), refund("R1", "1200")],
        [bank("S1", "GW-T1"), bank("S2", "GW-R1", gross="-1200", fee="0", net="-1200", on="2026-01-04")], [order()])
    f = flow(con, "TXN:R1")
    assert f["flow_type"] == "REFUND" and f["classification"] == "REFUND_MISMATCH" and f["first_breakpoint"] == "GATEWAY"
    assert f["gap_gateway"] == -200 and f["exposure_amount"] == 200 and f["bank_net"] == -1200
    assert f["order_amount"] is None and f["expected_net"] == -1200                    # refunds carry no fee


def test_two_refunds_that_together_exceed_the_payment_flag_only_the_excess(con):
    run(con, [gw("T1"), refund("R1", "700"), refund("R2", "500", created="2026-01-04T10:00:00")],
        [bank("S1", "GW-T1"), bank("S2", "GW-R1", gross="-700", fee="0", net="-700", on="2026-01-04"),
         bank("S3", "GW-R2", gross="-500", fee="0", net="-500", on="2026-01-05")], [order()])
    assert flow(con, "TXN:R1")["classification"] == "MATCHED"
    r2 = flow(con, "TXN:R2")
    assert r2["classification"] == "REFUND_MISMATCH" and r2["exposure_amount"] == 200        # 700 + 500 - 1000


def test_a_refund_the_bank_settled_for_the_wrong_amount_breaks_at_the_bank(con):
    run(con, [gw("T1"), refund("R1", "1000")],
        [bank("S1", "GW-T1"), bank("S2", "GW-R1", gross="-600", fee="0", net="-600", on="2026-01-04")], [order()])
    f = flow(con, "TXN:R1")
    assert f["classification"] == "REFUND_MISMATCH" and f["first_breakpoint"] == "BANK_SETTLEMENT"
    assert f["gap_bank_amount"] == 400 and f["exposure_amount"] == 400


def test_a_refund_of_a_payment_that_exists_nowhere_is_an_orphan_refund(con):
    run(con, [refund("R1", "1000", of="NOPE")], [bank("S2", "GW-R1", gross="-1000", fee="0", net="-1000", on="2026-01-04")])
    f = flow(con, "TXN:R1")
    assert f["classification"] == "REFUND_MISMATCH" and f["root_cause"] == "REFUND_OF_UNKNOWN_PAYMENT"
    assert f["exposure_amount"] == 1000


# ---- 11. quarantine boundary ---------------------------------------------------------------------------------------------
def quarantine_gateway(con, txn_id, order_id="O1", **raw):
    payload = {"txn_id": txn_id, "order_id": order_id, "merchant_id": "M1", "amount": "1000", "currency": "INR",
               "status": "captured", "method": "upi", "txn_type": "payment", "refund_of": None,
               "created_at": "2026-01-01T10:00:00", **raw}
    quarantine.replace(con, "rq", date(2026, 1, 1), "gateway", pd.DataFrame(
        [{"natural_key": txn_id, "order_id": order_id, "rule_ids": "GW-VAL-04", "raw_json": json.dumps(payload)}]))


def test_a_quarantined_gateway_record_blocks_the_chain_and_its_bank_money_is_not_an_orphan(con):
    quarantine_gateway(con, "T1", status="captred")
    run(con, [], [bank("S1", "GW-T1")], [order()])
    q = flow(con, "QTX:T1")
    assert q["flow_type"] == "QUARANTINED_PAYMENT" and q["classification"] == "QUARANTINED_SOURCE"
    assert q["first_breakpoint"] == "SOURCE_DATA_QUALITY" and q["resolution"] == "BLOCKED" and q["evidence_level"] == "LOW"
    assert q["order_amount"] == 1000 and q["bank_net"] == 980
    assert q["exposure_amount"] == 980                                                # one amount in question, not 1000 + 980
    assert con.execute("SELECT count(*) FROM gold.fact_money_flow WHERE flow_id LIKE 'STL:%' OR flow_id LIKE 'ORD:%'").fetchone()[0] == 0
    assert total_exposure(con) == 980


def test_a_quarantined_bank_record_is_reported_as_quarantined_not_as_missing(con):
    quarantine.replace(con, "rq", date(2026, 1, 2), "settlements", pd.DataFrame(
        [{"natural_key": "S1", "order_id": None, "rule_ids": "ST-VAL-02",
          "raw_json": json.dumps({"settlement_id": "S1", "gateway_ref": "GW-T1", "gross_amount": "??", "settlement_date": "2026-01-02"})}]))
    run(con, [gw("T1")], [], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "QUARANTINED_SOURCE" and f["root_cause"] == "SETTLEMENT_RECORD_QUARANTINED"
    assert f["first_breakpoint"] == "SOURCE_DATA_QUALITY" and f["resolution"] == "BLOCKED"
    ev = con.execute("SELECT record_ref FROM gold.money_flow_evidence WHERE flow_id = 'TXN:T1' AND evidence_type = 'QUARANTINE'").fetchall()
    assert ev == [("settlements:S1",)]


def test_a_bank_batch_the_quality_gate_refused_to_load_blocks_the_missing_verdict(con):
    con.execute("INSERT INTO ops.dq_scores VALUES ('rh', DATE '2026-01-02', 'settlements', 0.5, 'FAIL', 100, 0, false)")
    run(con, [gw("T1")], [], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "DATA_QUALITY_BLOCK" and f["resolution"] == "BLOCKED" and f["first_breakpoint"] == "SOURCE_DATA_QUALITY"
    con.execute("INSERT INTO ops.dq_scores VALUES ('rh2', DATE '2026-01-02', 'settlements', 0.99, 'PASS', 100, 0, true)")
    moneyflow.build(con, "again")                                                     # the batch was loaded on a re-run
    assert flow(con, "TXN:T1")["classification"] == "MISSING_SETTLEMENT"


# ---- 12. payment without order / 13. order without payment / 14. orphan settlement --------------------------------------------
def test_a_payment_with_no_order_breaks_at_the_order_side(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1")], [])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "PAYMENT_WITHOUT_ORDER" and f["first_breakpoint"] == "ORDER" and f["order_state"] == "ABSENT"
    assert f["gap_gateway"] == 1000 and f["difference_amount"] == 0 and f["exposure_amount"] == 1000


def test_a_paid_order_with_no_payment_breaks_at_the_gateway(con):
    run(con, [], [], [order()])
    f = flow(con, "ORD:O1")
    assert f["flow_type"] == "ORDER_ONLY" and f["classification"] == "ORDER_WITHOUT_PAYMENT" and f["first_breakpoint"] == "GATEWAY"
    assert f["gap_gateway"] == -1000 and f["exposure_amount"] == 1000 and f["gateway_gross"] is None


def test_a_bank_record_no_payment_can_claim_is_an_orphan_settlement(con):
    run(con, [], [bank("S9", "GW-UNKNOWN9999")], [])
    f = flow(con, "STL:S9")
    assert f["classification"] == "ORPHAN_SETTLEMENT" and f["first_breakpoint"] == "BANK_SETTLEMENT" and f["resolution"] == "OPEN"
    assert f["bank_net"] == 980 and f["exposure_amount"] == 980


# ---- 17. aggregated / 18. wrong reference ------------------------------------------------------------------------------------------
def test_one_bank_record_covering_two_payments_is_a_hypothesis_never_a_confirmed_match(con):
    # bank paid 1500 gross under T1's reference; T2 (500) has no record. 1000 + 500 = 1500, but the data has no batch id.
    run(con, [gw("T1"), gw("T2", order_id="O2", amount="500")], [bank("S1", "GW-T1", gross="1500", fee="30")],
        [order(), order("O2", amount="500")])
    a, b = flow(con, "TXN:T1"), flow(con, "TXN:T2")
    for f in (a, b):
        assert f["classification"] == "INSUFFICIENT_EVIDENCE" and f["first_breakpoint"] == "MATCHING"
        assert f["resolution"] == "BLOCKED" and f["root_cause"] == "POSSIBLE_AGGREGATED_SETTLEMENT" and f["evidence_level"] == "LOW"
    assert a["counted_in"] == "TXN:T2" and a["exposure_amount"] == 0 and approx(b["exposure_amount"], 490)
    assert total_exposure(con) == 490                                                  # the money in question, counted once


def test_the_same_amount_under_a_different_reference_is_a_candidate_not_a_match_and_is_counted_once(con):
    run(con, [gw("T1")], [bank("S9", "GW-ZZZZ777777", on="2026-01-04")], [order()])
    t, s = flow(con, "TXN:T1"), flow(con, "STL:S9")
    assert t["classification"] == "INSUFFICIENT_EVIDENCE" and t["root_cause"] == "CANDIDATE_BANK_RECORD_UNDER_DIFFERENT_REFERENCE"
    assert s["classification"] == "INSUFFICIENT_EVIDENCE" and s["counted_in"] == "TXN:T1"
    assert t["match_method"] is None and s["exposure_amount"] == 0 and t["exposure_amount"] == 980
    assert total_exposure(con) == 980
    ev = con.execute("SELECT evidence_type, record_ref FROM gold.money_flow_evidence WHERE flow_id = 'TXN:T1'").fetchall()
    assert ("CANDIDATE", "STL:S9") in ev


def test_a_bank_record_with_a_different_amount_and_reference_is_not_a_candidate(con):
    run(con, [gw("T1")], [bank("S9", "GW-ZZZZ777777", gross="400", fee="8")], [order()])
    assert flow(con, "TXN:T1")["classification"] == "MISSING_SETTLEMENT"
    assert flow(con, "STL:S9")["classification"] == "ORPHAN_SETTLEMENT"
    assert total_exposure(con) == 980 + 392


# ---- 20. unexplainable ---------------------------------------------------------------------------------------------------------------
def test_a_net_that_is_not_gross_minus_fee_is_the_residual_no_component_explains(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1", gross="1000", fee="20", net="975")], [order()])
    f = flow(con, "TXN:T1")
    assert f["classification"] == "UNEXPLAINED" and f["first_breakpoint"] == "BANK_SETTLEMENT" and f["resolution"] == "OPEN"
    assert f["gap_bank_amount"] == 0 and f["gap_fee"] == 0 and f["gap_bank_net"] == -5 and f["difference_amount"] == -5
    assert f["root_cause"] == "BANK_NET_IS_NOT_GROSS_MINUS_FEE" and f["exposure_amount"] == 5


# ---- traceability --------------------------------------------------------------------------------------------------------------------
def test_every_conclusion_can_be_traced_to_its_underlying_records(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1", gross="970", fee="19.4", net="950.6")], [order()])
    inv = moneyflow.investigate(con, "TXN:T1")
    stages = {s["stage"]: s for s in inv["stages"]}
    assert stages["ORDER"]["amount"] == 1000 and stages["GATEWAY"]["amount"] == 1000 and stages["PROCESSING"]["amount"] == 20
    assert stages["EXPECTED"]["amount"] == 980 and approx(stages["BANK_SETTLEMENT"]["amount"], 950.6)
    assert approx(stages["BANK_SETTLEMENT"]["difference"], -29.4)
    kinds = set(inv["evidence"]["evidence_type"])
    assert {"GATEWAY", "ORDER", "SETTLEMENT"} <= kinds
    assert inv["flow"]["settlement_ids"] == "S1" and inv["flow"]["bank_reference"] == "GW-T1"
    assert moneyflow.investigate(con, "TXN:NOPE") is None


def test_the_headline_answers_where_the_money_went_missing(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1", gross="970", fee="19.4", net="950.6")], [order()])
    text = moneyflow.headline(con)
    assert "INR 1,000 was expected from customers" in text and "INR 1,000 reached the gateway" in text
    assert "INR 980 should have settled" in text and "INR 951 reached the bank" in text
    assert "INR 29 is currently unexplained" in text and "earliest confirmed breakpoint is BANK_SETTLEMENT" in text
    assert "TXN:T1" in text and "not in the source data" in text


# ---- conservation across a mixed book ---------------------------------------------------------------------------------------------------
def test_no_money_is_counted_twice_and_the_bridge_closes_on_a_mixed_book(con):
    run(con,
        [gw("T1"), gw("T2", order_id="O2", amount="950"), gw("T3", order_id="O3"), gw("T4", order_id="O4"),
         gw("T5", order_id="O5"), gw("T6", order_id="O6", created="2026-01-01T10:03:00")],
        [bank("S1", "GW-T1"), bank("S2", "GW-T2", gross="950", fee="19"), bank("S3", "GW-T3", gross="970", fee="19.4"),
         bank("S4", "GW-T4", gross="1000", fee="50"), bank("S7", "GW-UNKNOWN1", gross="333.33", fee="6.67")],
        [order(), order("O2"), order("O3"), order("O4"), order("O5"), order("O7", amount="410")])
    inv = moneyflow.check_invariants(con)
    assert inv["ok"], inv
    rows = con.execute("SELECT flow_id, difference_amount, gap_bank_amount + gap_fee + gap_bank_net AS parts, exposure_amount "
                       "FROM gold.fact_money_flow").fetchall()
    assert all(abs(d - p) <= 0.02 for _, d, p, _ in rows)
    # bank money: every settlement row sits in exactly one chain, and the chains add up to the bank file
    assert con.execute("SELECT round(sum(bank_net), 2) FROM gold.fact_money_flow").fetchone()[0] == round(
        con.execute("SELECT sum(net_amount) FROM silver.settlements").fetchone()[0], 2)
    s = moneyflow.summary(con)["waterfall"]
    assert s["unexplained"] + s["blocked"] + s["explained"] == s["at_risk"]


def test_the_build_is_idempotent_and_replaces_rather_than_appends(con):
    run(con, [gw("T1")], [bank("S1", "GW-T1", gross="970", fee="19.4")], [order()])
    first = con.execute("SELECT * EXCLUDE (run_id) FROM gold.fact_money_flow ORDER BY flow_id").df()
    moneyflow.build(con, "second")
    moneyflow.build(con, "third")
    again = con.execute("SELECT * EXCLUDE (run_id) FROM gold.fact_money_flow ORDER BY flow_id").df()
    pd.testing.assert_frame_equal(first, again)
    assert con.execute("SELECT count(*) - count(DISTINCT flow_id) FROM gold.fact_money_flow").fetchone()[0] == 0
