"""Reconciliation completeness pass: suspected duplicates, the payment-without-order limitation, match evidence, explain.

These reuse the scenario helpers from test_reconcile.py (Silver is fed through the real upsert functions).
"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest
from test_reconcile import audit, gw, item, load, order, recon, stl

from payreconcile import explain, quarantine, reconcile


# ---- suspected duplicates (original quarantined) ---------------------------------------------------------------------
def quarantine_payment(con, key, order_id="O1", run="q", day=date(2026, 1, 1), **overrides):
    """Put a gateway payment attempt into quarantine with its raw payload, as ingestion would have stored it."""
    raw = gw(key, order_id=order_id, **overrides)
    q = pd.DataFrame([{"natural_key": key, "order_id": order_id, "rule_ids": "GW-VAL-04", "raw_json": json.dumps(raw)}])
    quarantine.replace(con, run, day, "gateway", q)


def dup_row(con, txn):
    return con.execute("SELECT is_duplicate_charge, is_suspected_duplicate, duplicate_basis, duplicate_of, "
                       "duplicate_exposure, suspected_duplicate_exposure FROM gold.fact_reconciliation "
                       "WHERE item_id = ?", [f"TXN:{txn}"]).fetchone()


def test_second_charge_whose_original_is_quarantined_is_suspected_not_confirmed(con):
    quarantine_payment(con, "T1", status="captred")                    # the original failed validation
    load(con, txns=[gw("T2", created="2026-01-01T10:03:00")], orders=[order("O1")])
    recon(con, "2026-01-02")
    assert dup_row(con, "T2") == (False, True, "QUARANTINED_ORIGINAL", "T1", 0, 100)
    assert reconcile.verify(con)["ok"]


@pytest.mark.parametrize("overrides,why", [
    ({"status": "failed"}, "a failed attempt followed by a success is normal, not a double charge"),
    ({"amount": "250"}, "a parseable, different amount contradicts the duplicate hypothesis"),
    ({"txn_type": "refund", "refund_of": "X"}, "a refund is not a second payment"),
])
def test_quarantined_rows_that_contradict_a_duplicate_do_not_flag_it(con, overrides, why):
    quarantine_payment(con, "T1", **overrides)
    load(con, txns=[gw("T2")], orders=[order("O1")])
    recon(con, "2026-01-02")
    assert dup_row(con, "T2")[:3] == (False, False, None), why


def test_unparseable_quarantined_fields_do_not_block_the_flag_but_a_different_order_does(con):
    quarantine_payment(con, "T1", amount="N/A", created="2026-02-30T10:00:00")     # nothing comparable, same order
    quarantine_payment(con, "T9", order_id="O9", run="q9", day=date(2026, 1, 2))   # another order entirely
    load(con, txns=[gw("T2"), gw("T3", order_id="O3")], orders=[order("O1"), order("O3")])
    recon(con, "2026-01-02")
    assert dup_row(con, "T2")[1] is True and dup_row(con, "T3")[1] is False


def test_a_webhook_copy_of_a_trusted_txn_is_never_evidence_of_a_second_charge(con):
    load(con, txns=[gw("T1")], orders=[order("O1")])
    quarantine_payment(con, "T1", run="qq")                                          # duplicate-webhook copy: same txn_id
    recon(con, "2026-01-02")
    assert dup_row(con, "T1")[:3] == (False, False, None)


def test_confirmed_pair_takes_precedence_and_releasing_the_original_upgrades_a_suspicion(con):
    quarantine_payment(con, "T1", status="captred")
    load(con, txns=[gw("T2", created="2026-01-01T10:03:00")], orders=[order("O1")])
    recon(con, "2026-01-02")
    assert dup_row(con, "T2")[2] == "QUARANTINED_ORIGINAL"
    quarantine.replace(con, "q2", date(2026, 1, 1), "gateway", None)                 # replay releases the original ...
    load(con, txns=[gw("T1", created="2026-01-01T10:00:00")])                        # ... into Silver
    recon(con, "2026-01-02")
    assert dup_row(con, "T2") == (True, False, "TRUSTED_PAIR", "T1", 100, 0)
    assert audit(con, "TXN:T2")[-1][0] == "ATTRIBUTE_CHANGE"                         # the change is on the audit trail
    assert reconcile.verify(con)["ok"]


# ---- payment without order: the documented limitation -----------------------------------------------------------------
def test_trusted_payment_with_no_order_is_flagged(con):
    load(con, txns=[gw("T1", order_id="ORPHAN")], stls=[stl("S1", "GW-T1")])
    recon(con, "2026-01-02")
    flag = con.execute("SELECT order_issue FROM gold.fact_reconciliation WHERE item_id = 'TXN:T1'").fetchone()[0]
    assert flag == "NO_ORDER"


def test_quarantined_payment_with_no_order_is_not_lost_but_surfaces_through_quarantine_and_the_settlement(con):
    quarantine_payment(con, "T1", order_id="ORPHAN")                                 # failed validation AND has no order
    load(con, stls=[stl("S1", "GW-T1")])
    recon(con, "2026-01-02")
    assert item(con, "TXN:T1") is None                                               # not a trusted item: no NO_ORDER flag
    assert item(con, "STL:S1")["status"] == "SOURCE_QUARANTINED"                     # the money movement is still visible
    assert con.execute("SELECT count(*) FROM quarantine.rows WHERE natural_key = 'T1'").fetchone()[0] == 1


# ---- explainability ----------------------------------------------------------------------------------------------------
def evidence(con, txn_id):
    return con.execute("SELECT method, decision, edit_distance, amount_diff, days_gap, merchant_match, txn_candidates, "
                       "settlement_candidates, score, threshold, reason FROM gold.recon_match_evidence "
                       "WHERE txn_id = ? ORDER BY settlement_id", [txn_id]).fetchall()


def test_exact_match_records_why_the_records_are_the_same_transaction(con):
    load(con, txns=[gw("T1")], stls=[stl("S1", "gw-t1", on="2026-01-02")])
    recon(con, "2026-01-02")
    (row,) = evidence(con, "T1")
    assert row[:9] == ("exact", "LINKED", 0, 0.0, 1, True, 1, 1, 1.0)
    assert "normalised bank reference" in row[10] and "max_edit_distance=2" in row[9]


def test_fuzzy_match_records_features_score_threshold_decision_and_reason(con):
    load(con, txns=[gw("TXN1234567890")], stls=[stl("S1", "GW-TXN1234567899")])
    recon(con, "2026-01-02")
    (row,) = evidence(con, "TXN1234567890")
    assert row[:9] == ("fuzzy", "LINKED", 1, 0.0, 1, True, 1, 1, pytest.approx(1 - 1 / 3, abs=1e-3))
    assert "unique candidate from both sides" in row[10] and "differ by 1 character" in row[10]


def test_ambiguous_fuzzy_candidates_are_all_recorded_with_the_reason_no_match_was_made(con):
    load(con, txns=[gw("TXN1234567890", order_id="O1"), gw("TXN1234567891", order_id="O2")],
         stls=[stl("S1", "GW-TXN1234567899")])
    recon(con, "2026-01-02")
    rows = con.execute("SELECT txn_id, decision, txn_candidates, settlement_candidates, reason "
                       "FROM gold.recon_match_evidence ORDER BY txn_id").fetchall()
    assert [r[:4] for r in rows] == [("TXN1234567890", "AMBIGUOUS_NOT_LINKED", 1, 2),
                                     ("TXN1234567891", "AMBIGUOUS_NOT_LINKED", 1, 2)]
    assert "no match was made" in rows[0][4]
    assert con.execute("SELECT count(*) FROM gold.recon_links").fetchone()[0] == 0


def test_evidence_is_replaced_when_a_better_candidate_arrives_and_resolves_the_ambiguity(con):
    load(con, txns=[gw("TXN1234567890", order_id="O1"), gw("TXN1234567891", order_id="O2")],
         stls=[stl("S1", "GW-TXN1234567899")])
    recon(con, "2026-01-02")
    load(con, stls=[stl("S1", "GW-TXN1234567890")])                                  # the bank corrects the reference
    recon(con, "2026-01-03")
    assert [r[:2] for r in evidence(con, "TXN1234567890")] == [("exact", "LINKED")]  # ambiguity rows are gone
    assert item(con, "TXN:TXN1234567890")["status"] == "MATCHED"
    assert reconcile.verify(con)["ok"]


def test_explain_returns_evidence_audit_history_and_source_records(con):
    load(con, txns=[gw("TXN1234567890")], stls=[stl("S1", "GW-TXN1234567899")], orders=[order("O1")])
    recon(con, "2026-01-02")
    out = explain.explain(con, "TXN:TXN1234567890", include_bronze=False)
    assert out["current"]["status"] == "MATCHED" and out["current"]["match_method"] == "fuzzy"
    assert out["match_evidence"][0]["decision"] == "LINKED"
    assert out["audit_history"][0]["change_kind"] == "NEW_ITEM"
    assert out["silver_records"]["gateway_txns"][0]["txn_id"] == "TXN1234567890"
    assert out["silver_records"]["settlements"][0]["settlement_id"] == "S1"
