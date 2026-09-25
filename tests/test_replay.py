"""Quarantine replay: release what is now valid, keep what is not, never double-release, never overwrite newer data."""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
from test_reconcile import gw, item, load, order, recon, stl

from payreconcile import db, quarantine, reconcile, replay

DAY = date(2026, 1, 1)


def put(con, rows: list[dict], dataset="gateway", rule="GW-REF-01", key="txn_id", day=DAY, run="q0"):
    """Place raw records into quarantine exactly as ingestion would have stored them."""
    q = pd.DataFrame([{"natural_key": r[key], "order_id": r.get("order_id"), "rule_ids": rule, "raw_json": json.dumps(r)}
                      for r in rows])
    quarantine.replace(con, run, day, dataset, q)


def add_merchant(con, mid="M9"):
    con.execute("""INSERT INTO silver.merchants_scd2 VALUES (?, 'New shop', 'food', 'Pune', 'low', 2.0,
                   DATE '1970-01-01', DATE '9999-12-31', true, 'h', 'seed')""", [mid])


def run_replay(con, dataset="gateway", day=DAY):
    con.close()                                    # replay opens its own connection
    return replay.replay(dataset, day, run_dbt=False)


def reopen():
    return db.connect()


def q_rows(con, dataset="gateway"):
    return con.execute("SELECT natural_key, rule_ids FROM quarantine.rows WHERE dataset = ? ORDER BY natural_key", [dataset]).fetchall()


def log_rows(con):
    return con.execute("SELECT natural_key, outcome, old_rule_ids, new_rule_ids FROM ops.replay_log ORDER BY replayed_at, natural_key").fetchall()


def test_previously_invalid_record_becomes_valid_and_downstream_reconciliation_updates(con):
    put(con, [gw("T1", merchant="M9", order_id="O1")])
    load(con, stls=[stl("S1", "GW-T1", merchant="M9")], orders=[order("O1", merchant="M9")])
    recon(con, "2026-01-02")
    assert item(con, "STL:S1")["status"] == "SOURCE_QUARANTINED"           # the bank paid for a payment we rejected
    add_merchant(con)                                                       # reference data arrives
    out = run_replay(con)
    assert (out["candidates"], out["released"], out["still_quarantined"]) == (1, 1, 0)
    assert out["reconciliation"]["mode"] == "incremental"                   # Gold was updated, not left stale until tomorrow
    con = reopen()
    assert con.execute("SELECT txn_id FROM silver.gateway_txns").fetchall() == [("T1",)]
    assert q_rows(con) == []
    assert item(con, "TXN:T1")["status"] == "MATCHED" and item(con, "STL:S1") is None
    assert log_rows(con) == [("T1", "RELEASED", "GW-REF-01", None)]
    assert reconcile.verify(con)["ok"]


def test_record_that_remains_invalid_stays_and_its_rule_ids_are_refreshed(con):
    put(con, [gw("T1", merchant="M9", amount="-5")], rule="GW-REF-01,GW-VAL-03")
    add_merchant(con)                                                       # fixes ONE of the two reasons
    out = run_replay(con)
    assert (out["released"], out["still_quarantined"]) == (0, 1)
    con = reopen()
    assert q_rows(con) == [("T1", "GW-VAL-03")]                             # only the remaining reason is left
    assert log_rows(con) == [("T1", "STILL_INVALID", "GW-REF-01,GW-VAL-03", "GW-VAL-03")]
    assert con.execute("SELECT count(*) FROM silver.gateway_txns").fetchone()[0] == 0


def test_duplicate_quarantine_copy_is_never_replayed(con):
    load(con, txns=[gw("T1")])
    put(con, [gw("T1")], rule="GW-UNQ-01")                                  # the second copy of a duplicate webhook
    out = run_replay(con)
    assert out["candidates"] == 0
    con = reopen()
    assert q_rows(con) == [("T1", "GW-UNQ-01")]
    assert con.execute("SELECT count(*) FROM silver.gateway_txns").fetchone()[0] == 1


def test_schema_rejected_input_is_not_replayed(con):
    put(con, [gw("T1")], rule="SCHEMA_REJECT")
    out = run_replay(con)
    assert out["candidates"] == 0
    con = reopen()
    assert q_rows(con) == [("T1", "SCHEMA_REJECT")] and con.execute("SELECT count(*) FROM silver.gateway_txns").fetchone()[0] == 0


def test_replay_rerun_releases_nothing_twice_and_changes_nothing(con):
    put(con, [gw("T1", merchant="M9", order_id="O1")])
    add_merchant(con)
    first = run_replay(con)
    assert first["released"] == 1
    con = reopen()
    seq = con.execute("SELECT max(seq) FROM ops.change_log").fetchone()[0]
    audit_n = con.execute("SELECT count(*) FROM gold.reconciliation_audit").fetchone()[0]
    second = run_replay(con)
    assert (second["candidates"], second["released"]) == (0, 0)
    con = reopen()
    assert con.execute("SELECT count(*) FROM silver.gateway_txns").fetchone()[0] == 1          # not duplicated
    assert con.execute("SELECT max(seq) FROM ops.change_log").fetchone()[0] == seq              # nothing new logged
    assert con.execute("SELECT count(*) FROM gold.reconciliation_audit").fetchone()[0] == audit_n
    assert [r[1] for r in log_rows(con)] == ["RELEASED"]                                        # one release, ever


def test_reference_data_change_releases_orders_of_a_newly_onboarded_merchant(con):
    o = order("O1", merchant="M9")
    put(con, [o], dataset="orders", rule="OR-REF-01", key="order_id")
    add_merchant(con)
    out = run_replay(con, "orders")
    assert out["released"] == 1
    con = reopen()
    assert con.execute("SELECT order_id, merchant_id FROM silver.orders").fetchall() == [("O1", "M9")]
    assert q_rows(con, "orders") == []


def test_replay_never_overwrites_newer_trusted_data_with_a_stale_quarantined_copy(con):
    put(con, [gw("T1", amount="100")], rule="GW-VAL-04")                    # old version rejected earlier
    load(con, txns=[gw("T1", amount="120")])                                # the corrected record was re-sent and loaded
    out = run_replay(con)
    assert (out["released"], out["superseded"]) == (0, 1)
    con = reopen()
    assert con.execute("SELECT amount FROM silver.gateway_txns WHERE txn_id = 'T1'").fetchone()[0] == 120.0
    assert q_rows(con) == [("T1", "SUPERSEDED_BY_SILVER")]                  # kept for audit, skipped from now on
    assert log_rows(con)[0][:2] == ("T1", "SUPERSEDED")
    assert run_replay(con)["candidates"] == 0                               # a later replay ignores it


def test_a_mixed_batch_releases_only_the_valid_rows(con):
    put(con, [gw("T1", merchant="M9", order_id="O1"), gw("T2", merchant="M9", order_id="O2", amount="-1")])
    add_merchant(con)
    out = run_replay(con)
    assert (out["candidates"], out["released"], out["still_quarantined"]) == (2, 1, 1)
    con = reopen()
    assert [r[0] for r in con.execute("SELECT txn_id FROM silver.gateway_txns").fetchall()] == ["T1"]
    assert [r[0] for r in q_rows(con)] == ["T2"]


def test_replay_history_is_append_only_across_attempts(con):
    put(con, [gw("T1", merchant="M9", amount="-5")], rule="GW-REF-01,GW-VAL-03")
    run_replay(con)
    con = reopen()
    run_replay(con)                                                         # another attempt on the same bad row
    con = reopen()
    rows = log_rows(con)
    assert len(rows) == 2 and {r[1] for r in rows} == {"STILL_INVALID"}    # both attempts are on record
    con.close()
