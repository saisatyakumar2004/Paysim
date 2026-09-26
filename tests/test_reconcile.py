"""Incremental reconciliation: late arrivals, stable matches, audit trail, idempotency, atomicity.

Every scenario feeds Silver through the real upsert functions (so ops.change_log is populated exactly as in production)
and then runs the reconciliation as-of a given day.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from payreconcile import quarantine, reconcile, silver


# ---- helpers ------------------------------------------------------------------------------------------------
def gw(txn_id, order_id="O1", merchant="M1", amount="100", created="2026-01-01T10:00:00", status="captured",
       txn_type="payment", refund_of=None):
    return {"txn_id": txn_id, "order_id": order_id, "merchant_id": merchant, "amount": amount, "currency": "INR",
            "status": status, "method": "upi", "txn_type": txn_type, "refund_of": refund_of, "created_at": created}


def stl(sid, ref, amount="100", merchant="M1", on="2026-01-02"):
    return {"settlement_id": sid, "gateway_ref": ref, "merchant_id": merchant, "gross_amount": amount, "fee_amount": "2",
            "net_amount": str(float(amount) - 2), "settlement_date": on}


def order(oid, amount="100", status="paid", created="2026-01-01 09:00:00", merchant="M1"):
    return {"order_id": oid, "merchant_id": merchant, "customer_id": "C1", "order_amount": amount,
            "order_status": status, "created_at": created, "updated_at": created}


def load(con, txns=(), stls=(), orders=(), run="r"):
    if txns:
        silver.apply_gateway(con, pd.DataFrame(list(txns)).astype(object), run)
    if stls:
        silver.apply_settlements(con, pd.DataFrame(list(stls)).astype(object), run)
    if orders:
        silver.apply_orders(con, pd.DataFrame(list(orders)).astype(object), run)


_runs = iter(range(1, 10_000))


def recon(con, as_of: str, **kw):
    return reconcile.reconcile(con, date.fromisoformat(as_of), f"run{next(_runs)}", date.fromisoformat(as_of), **kw)


def item(con, item_id):
    row = con.execute("SELECT status, match_method, updated_run_id, status_since FROM gold.fact_reconciliation "
                      "WHERE item_id = ?", [item_id]).fetchone()
    return None if row is None else {"status": row[0], "method": row[1], "run": row[2], "since": row[3]}


def audit(con, item_id):
    return con.execute("SELECT change_kind, reason, old_status, new_status FROM gold.reconciliation_audit "
                       "WHERE item_id = ? ORDER BY audit_id", [item_id]).fetchall()


def audit_count(con):
    return con.execute("SELECT count(*) FROM gold.reconciliation_audit").fetchone()[0]


# ---- late-arriving settlements ---------------------------------------------------------------------------------
def test_pending_becomes_matched_when_the_settlement_arrives(con):
    load(con, txns=[gw("T1")], orders=[order("O1")])
    recon(con, "2026-01-01")
    assert item(con, "TXN:T1")["status"] == "PENDING"
    load(con, stls=[stl("S1", "GW-T1")])
    s = recon(con, "2026-01-02")
    assert item(con, "TXN:T1")["status"] == "MATCHED" and s.mode == "incremental"
    assert audit(con, "TXN:T1") == [("NEW_ITEM", "NEW_ITEM", None, "PENDING"),
                                    ("STATUS_CHANGE", "INPUT_CHANGE", "PENDING", "MATCHED")]


def test_unsettled_txn_ages_into_missing_and_a_very_late_settlement_still_resolves_it(con):
    load(con, txns=[gw("T1")])
    recon(con, "2026-01-01")
    s = recon(con, "2026-01-05")                            # SLA (3 days) has elapsed with no input change at all
    assert item(con, "TXN:T1")["status"] == "MISSING_SETTLEMENT"
    assert s.changes_read == 0 and s.scope_txns == 1        # only the aged item was touched
    assert audit(con, "TXN:T1")[-1] == ("STATUS_CHANGE", "AGED", "PENDING", "MISSING_SETTLEMENT")
    load(con, stls=[stl("S1", "GW-T1", on="2026-01-06")])   # the bank finally pays, days late
    recon(con, "2026-01-06")
    assert item(con, "TXN:T1")["status"] == "MATCHED"
    assert audit(con, "TXN:T1")[-1] == ("STATUS_CHANGE", "INPUT_CHANGE", "MISSING_SETTLEMENT", "MATCHED")
    assert reconcile.verify(con)["ok"]


def test_settlement_before_the_gateway_record_is_an_orphan_until_the_txn_arrives(con):
    load(con, stls=[stl("S1", "GW-T1")])
    recon(con, "2026-01-02")
    assert item(con, "STL:S1")["status"] == "ORPHAN_SETTLEMENT"
    load(con, txns=[gw("T1")])                              # late-arriving gateway record
    recon(con, "2026-01-03")
    assert item(con, "STL:S1") is None and item(con, "TXN:T1")["status"] == "MATCHED"
    assert audit(con, "STL:S1")[-1] == ("CLOSED", "RESOLVED", "ORPHAN_SETTLEMENT", "CLOSED")
    assert reconcile.verify(con)["ok"]


def test_corrected_settlement_amount_clears_a_mismatch(con):
    load(con, txns=[gw("T1")], stls=[stl("S1", "GW-T1", amount="60")])
    recon(con, "2026-01-02")
    assert item(con, "TXN:T1")["status"] == "AMOUNT_MISMATCH"
    load(con, stls=[stl("S1", "GW-T1", amount="100")])      # same settlement id, corrected by the bank
    recon(con, "2026-01-03")
    assert item(con, "TXN:T1")["status"] == "MATCHED"
    assert reconcile.verify(con)["ok"]


# ---- late-arriving refunds ------------------------------------------------------------------------------------------
def test_refund_before_its_original_is_an_orphan_and_resolves_when_the_original_arrives(con):
    load(con, txns=[gw("R1", txn_type="refund", refund_of="T1", created="2026-01-02T10:00:00", order_id="O1")])
    recon(con, "2026-01-02")
    assert item(con, "TXN:R1")["status"] == "ORPHAN_REFUND"
    load(con, txns=[gw("T1")], stls=[stl("S1", "GW-T1"), stl("S2", "GW-R1", amount="-100", on="2026-01-03")])
    recon(con, "2026-01-03")
    assert item(con, "TXN:R1")["status"] == "MATCHED"
    assert audit(con, "TXN:R1")[-1][2:] == ("ORPHAN_REFUND", "MATCHED")
    assert reconcile.verify(con)["ok"]


def test_a_refund_arriving_later_does_not_reopen_the_resolved_original(con):
    load(con, txns=[gw("T1")], stls=[stl("S1", "GW-T1")], orders=[order("O1")])
    recon(con, "2026-01-02")
    before = (item(con, "TXN:T1"), audit(con, "TXN:T1"))
    load(con, txns=[gw("R1", txn_type="refund", refund_of="T1", created="2026-01-04T10:00:00")],
         stls=[stl("S2", "GW-R1", amount="-100", on="2026-01-05")])
    s = recon(con, "2026-01-05")
    assert (item(con, "TXN:T1"), audit(con, "TXN:T1")) == before      # untouched: same row, same run id, no audit rows
    assert item(con, "TXN:R1")["status"] == "MATCHED"
    assert s.scope_txns < 4


# ---- stability / idempotency ---------------------------------------------------------------------------------------------
def test_unrelated_changes_do_not_re_evaluate_resolved_items(con):
    load(con, txns=[gw("T1"), gw("T2", order_id="O2")], stls=[stl("S1", "GW-T1"), stl("S2", "GW-T2")])
    recon(con, "2026-01-02")
    frozen = {i: item(con, i) for i in ("TXN:T1", "TXN:T2")}
    n_audit = audit_count(con)
    load(con, txns=[gw("T3", order_id="O3", created="2026-01-03T10:00:00")])
    s = recon(con, "2026-01-03")
    assert {i: item(con, i) for i in frozen} == frozen
    assert s.scope_txns == 1 and s.items_evaluated == 1              # only the new txn
    assert audit_count(con) == n_audit + 1                           # exactly one new audit row: T3 NEW_ITEM


def test_rerunning_with_no_new_data_changes_nothing(con):
    load(con, txns=[gw("T1"), gw("T2", order_id="O2")], stls=[stl("S1", "GW-T1")], orders=[order("O1"), order("O2")])
    recon(con, "2026-01-02")
    snap = con.execute("SELECT * FROM gold.fact_reconciliation ORDER BY item_id").fetchall()
    n_audit = audit_count(con)
    for _ in range(3):
        s = recon(con, "2026-01-02")
        assert s.items_changed == 0
    assert con.execute("SELECT * FROM gold.fact_reconciliation ORDER BY item_id").fetchall() == snap
    assert audit_count(con) == n_audit


def test_re_applying_identical_silver_rows_is_a_no_op(con):
    rows = [gw("T1")]
    load(con, txns=rows)
    seq = con.execute("SELECT max(seq) FROM ops.change_log").fetchone()[0]
    silver.apply_gateway(con, pd.DataFrame(rows).astype(object), "another-run")
    assert con.execute("SELECT max(seq) FROM ops.change_log").fetchone()[0] == seq     # nothing logged, nothing rewritten
    assert con.execute("SELECT _run_id FROM silver.gateway_txns").fetchone()[0] == "r"


def test_replay_of_an_old_day_does_not_rewind_the_clock(con):
    load(con, txns=[gw("T1")])
    recon(con, "2026-01-10")
    assert item(con, "TXN:T1")["status"] == "MISSING_SETTLEMENT"
    s = recon(con, "2026-01-02")                                      # replay of an early day
    assert s.as_of == date(2026, 1, 10) and item(con, "TXN:T1")["status"] == "MISSING_SETTLEMENT"


# ---- matches ---------------------------------------------------------------------------------------------------------------
def test_fuzzy_match_links_a_typoed_reference(con):
    load(con, txns=[gw("TXN1234567890")], stls=[stl("S1", "GW-TXN1234567899")])
    recon(con, "2026-01-02")
    assert item(con, "TXN:TXN1234567890") == {**item(con, "TXN:TXN1234567890"), "status": "MATCHED", "method": "fuzzy"}
    assert item(con, "STL:S1") is None
    assert reconcile.verify(con)["ok"]


def test_ambiguous_fuzzy_candidates_are_never_guessed(con):
    load(con, txns=[gw("TXN1234567890", order_id="O1"), gw("TXN1234567891", order_id="O2")],
         stls=[stl("S1", "GW-TXN1234567899")])
    recon(con, "2026-01-02")
    assert item(con, "STL:S1")["status"] == "ORPHAN_SETTLEMENT"      # two equally plausible txns -> stay open
    assert reconcile.verify(con)["ok"]


def test_a_resolved_fuzzy_match_is_sticky_when_a_competing_candidate_arrives_later(con):
    load(con, txns=[gw("TXN1234567890", order_id="O1")], stls=[stl("S1", "GW-TXN1234567899")])
    recon(con, "2026-01-02")
    assert item(con, "TXN:TXN1234567890")["method"] == "fuzzy"
    load(con, txns=[gw("TXN1234567891", order_id="O2", created="2026-01-01T11:00:00")])   # a lookalike shows up
    recon(con, "2026-01-02")
    assert item(con, "TXN:TXN1234567890")["status"] == "MATCHED"      # not reopened
    assert item(con, "TXN:TXN1234567891")["status"] == "PENDING"
    # documented trade-off: a from-scratch rebuild would see two candidates and leave both unmatched
    assert not reconcile.verify(con)["ok"]


# ---- order-side and quarantine dependencies ---------------------------------------------------------------------------------------
def test_payment_without_order_clears_when_the_order_arrives_late(con):
    load(con, txns=[gw("T1")], stls=[stl("S1", "GW-T1")])
    recon(con, "2026-01-02")
    assert con.execute("SELECT order_issue FROM gold.fact_reconciliation WHERE item_id = 'TXN:T1'").fetchone()[0] == "NO_ORDER"
    load(con, orders=[order("O1")])
    recon(con, "2026-01-03")
    assert con.execute("SELECT order_issue FROM gold.fact_reconciliation WHERE item_id = 'TXN:T1'").fetchone()[0] is None
    assert reconcile.verify(con)["ok"]


def test_paid_order_without_payment_is_flagged_only_after_it_is_old_enough_and_clears_on_payment(con):
    load(con, orders=[order("O9", created="2026-01-01 09:00:00")])
    recon(con, "2026-01-02")
    assert item(con, "ORD:O9") is None                                # too young to judge
    recon(con, "2026-01-05")                                          # time alone makes it eligible
    assert item(con, "ORD:O9")["status"] == "ORDER_WITHOUT_PAYMENT"
    load(con, txns=[gw("T9", order_id="O9", created="2026-01-04T10:00:00")])
    recon(con, "2026-01-05")
    assert item(con, "ORD:O9") is None and reconcile.verify(con)["ok"]


def test_duplicate_charge_flag_appears_when_the_earlier_charge_arrives_late(con):
    load(con, txns=[gw("T2", created="2026-01-01T10:03:00")], orders=[order("O1")])
    recon(con, "2026-01-01")
    flag = "SELECT is_duplicate_charge FROM gold.fact_reconciliation WHERE item_id = 'TXN:T2'"
    assert con.execute(flag).fetchone()[0] is False
    load(con, txns=[gw("T1", created="2026-01-01T10:00:00")])         # the first charge shows up afterwards
    recon(con, "2026-01-02")
    assert con.execute(flag).fetchone()[0] is True
    assert reconcile.verify(con)["ok"]


def test_quarantined_payment_is_not_an_orphan_and_releasing_it_resolves_everything(con):
    q = pd.DataFrame([{"natural_key": "T1", "order_id": "O1", "rule_ids": "GW-VAL-04", "raw_json": "{}"}])
    quarantine.replace(con, "q1", date(2026, 1, 1), "gateway", q)
    load(con, stls=[stl("S1", "GW-T1")], orders=[order("O1")])
    recon(con, "2026-01-05")
    assert item(con, "STL:S1")["status"] == "SOURCE_QUARANTINED"
    assert item(con, "ORD:O1")["status"] == "PAYMENT_QUARANTINED"
    quarantine.replace(con, "q2", date(2026, 1, 1), "gateway", None)  # replay released it ...
    load(con, txns=[gw("T1")])                                         # ... into Silver
    recon(con, "2026-01-05")
    assert item(con, "TXN:T1")["status"] == "MATCHED"
    assert item(con, "STL:S1") is None and item(con, "ORD:O1") is None
    assert reconcile.verify(con)["ok"]


# ---- audit trail --------------------------------------------------------------------------------------------------------------------
def test_audit_trail_is_append_only_and_explains_the_current_state(con):
    load(con, txns=[gw("T1"), gw("T2", order_id="O2")], orders=[order("O1"), order("O2")])
    recon(con, "2026-01-01")
    recon(con, "2026-01-05")
    load(con, stls=[stl("S1", "GW-T1", on="2026-01-06")])
    recon(con, "2026-01-06")
    ids_before = [r[0] for r in con.execute("SELECT audit_id FROM gold.reconciliation_audit ORDER BY audit_id").fetchall()]
    recon(con, "2026-01-07")
    ids_after = [r[0] for r in con.execute("SELECT audit_id FROM gold.reconciliation_audit ORDER BY audit_id").fetchall()]
    assert ids_after[:len(ids_before)] == ids_before                  # earlier history is never rewritten
    last = dict(con.execute("SELECT item_id, arg_max(new_status, audit_id) FROM gold.reconciliation_audit "
                            "GROUP BY item_id").fetchall())
    state = dict(con.execute("SELECT item_id, status FROM gold.fact_reconciliation").fetchall())
    assert {k: v for k, v in last.items() if v != "CLOSED"} == state
    assert [a[3] for a in audit(con, "TXN:T1")] == ["PENDING", "MISSING_SETTLEMENT", "MATCHED"]
    assert con.execute("SELECT status_since FROM gold.fact_reconciliation WHERE item_id = 'TXN:T1'").fetchone()[0] == date(2026, 1, 6)


# ---- atomicity ----------------------------------------------------------------------------------------------------------------------------
class FlakyConnection:
    """Delegates to a real connection but fails the moment the links table is written (mid-apply)."""

    def __init__(self, con):
        self._con = con

    def execute(self, sql, *args):
        if "INSERT INTO gold.recon_links" in sql:
            raise RuntimeError("simulated crash mid-apply")
        return self._con.execute(sql, *args)

    def executemany(self, sql, *args):
        return self._con.executemany(sql, *args)


def test_a_crash_mid_apply_leaves_the_previous_state_and_a_rerun_recovers(con):
    load(con, txns=[gw("T1")])
    recon(con, "2026-01-01")
    state_before = con.execute("SELECT * FROM gold.fact_reconciliation ORDER BY item_id").fetchall()
    audit_before, wm_before = audit_count(con), con.execute("SELECT last_seq FROM ops.recon_state").fetchone()
    load(con, stls=[stl("S1", "GW-T1")])
    with pytest.raises(RuntimeError, match="simulated crash"):
        reconcile.reconcile(FlakyConnection(con), date(2026, 1, 2), "boom", date(2026, 1, 2))
    assert con.execute("SELECT * FROM gold.fact_reconciliation ORDER BY item_id").fetchall() == state_before
    assert audit_count(con) == audit_before
    assert con.execute("SELECT last_seq FROM ops.recon_state").fetchone() == wm_before   # watermark did NOT advance
    recon(con, "2026-01-02")                                                             # the retry picks the change up
    assert item(con, "TXN:T1")["status"] == "MATCHED" and reconcile.verify(con)["ok"]


def test_forced_full_recompute_repairs_a_corrupted_state_and_audits_the_repair(con):
    load(con, txns=[gw("T1")], stls=[stl("S1", "GW-T1")])
    recon(con, "2026-01-02")
    con.execute("UPDATE gold.fact_reconciliation SET status = 'MISSING_SETTLEMENT' WHERE item_id = 'TXN:T1'")
    assert not reconcile.verify(con)["ok"]
    s = recon(con, "2026-01-02", full=True)
    assert s.mode == "full" and item(con, "TXN:T1")["status"] == "MATCHED" and reconcile.verify(con)["ok"]
    assert audit(con, "TXN:T1")[-1][2:] == ("MISSING_SETTLEMENT", "MATCHED")


# ---- one txn referenced by several bank records ---------------------------------------------------------------------
def test_a_txn_paid_twice_by_the_bank_is_one_item_and_a_mismatch_not_a_hidden_match(con):
    load(con, txns=[gw("T1")], orders=[order("O1")],
         stls=[stl("S1", "GW-T1", on="2026-01-02"), stl("S2", "GW-T1", on="2026-01-03")])
    recon(con, "2026-01-05")
    rows = con.execute("SELECT status, settled_amount, diff_amount FROM gold.fact_reconciliation "
                       "WHERE item_id = 'TXN:T1'").fetchall()
    assert rows == [("AMOUNT_MISMATCH", 200.0, 100.0)]          # was: four identical MATCHED rows
    assert con.execute("SELECT count(*) - count(DISTINCT item_id) FROM gold.fact_reconciliation").fetchone()[0] == 0
    assert reconcile.verify(con)["ok"]


def test_a_settlement_split_over_two_bank_records_matches_once_when_the_parts_add_up(con):
    load(con, txns=[gw("T1")], orders=[order("O1")],
         stls=[stl("S1", "GW-T1", amount="60", on="2026-01-02"), stl("S2", "GW-T1", amount="40", on="2026-01-02")])
    recon(con, "2026-01-05")
    rows = con.execute("SELECT status, settled_amount FROM gold.fact_reconciliation WHERE item_id = 'TXN:T1'").fetchall()
    assert rows == [("MATCHED", 100.0)]
    assert con.execute("SELECT count(*) FROM gold.fact_reconciliation WHERE item_type = 'settlement'").fetchone()[0] == 0
    assert reconcile.verify(con)["ok"]
