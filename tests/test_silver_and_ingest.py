"""Silver upserts (idempotency, SCD2) and ingestion (retries, bronze overwrite)."""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest
from conftest import gw_df, gw_row

from payreconcile import config, ingest, silver


def merchants(**over):
    row = {"merchant_id": "M1", "name": "Shop 1", "category": "grocery", "city": "Pune", "risk_tier": "low",
           "fee_pct": "2.0"}
    row.update(over)
    return pd.DataFrame([row]).astype(object)


def versions(con, merchant_id="M1"):
    return con.execute("SELECT valid_from, valid_to, is_current, risk_tier FROM silver.merchants_scd2 "
                       "WHERE merchant_id = ? ORDER BY valid_from", [merchant_id]).fetchall()


def test_scd2_no_change_creates_no_new_version(con):
    silver.apply_merchants_scd2(con, merchants(), date(2026, 1, 8), "r1")
    assert len(versions(con)) == 1


def test_scd2_change_closes_old_row_and_opens_new_one(con):
    silver.apply_merchants_scd2(con, merchants(risk_tier="high"), date(2026, 1, 8), "r1")
    rows = versions(con)
    assert [r[2] for r in rows] == [False, True]                    # old closed, new current
    assert rows[0][1] == date(2026, 1, 8) == rows[1][0]             # valid_to of old == valid_from of new
    assert rows[1][1] == date(9999, 12, 31) and rows[1][3] == "high"


def test_scd2_replaying_the_same_snapshot_is_idempotent(con):
    for _ in range(3):
        silver.apply_merchants_scd2(con, merchants(risk_tier="high"), date(2026, 1, 8), "r")
    assert len(versions(con)) == 2


def test_scd2_older_snapshot_never_overwrites_a_newer_version(con):
    silver.apply_merchants_scd2(con, merchants(risk_tier="high"), date(2026, 1, 15), "r1")
    silver.apply_merchants_scd2(con, merchants(risk_tier="medium"), date(2026, 1, 8), "r2")   # stale replay
    assert versions(con)[-1][3] == "high" and len(versions(con)) == 2


def test_gateway_upsert_repairs_amounts_and_dates_and_is_idempotent(con):
    good = gw_df(gw_row(amount="₹1,299.00", created_at="02/01/2026 10:00:00"))
    for _ in range(2):
        silver.apply_gateway(con, good, "r1")
    rows = con.execute("SELECT amount, created_at FROM silver.gateway_txns").fetchall()
    assert len(rows) == 1 and rows[0][0] == 1299.0 and str(rows[0][1]) == "2026-01-02 10:00:00"


def test_orders_older_version_does_not_overwrite_newer(con):
    def order(status, updated):
        return pd.DataFrame([{"order_id": "O1", "merchant_id": "M1", "customer_id": "C1", "order_amount": "100",
                              "order_status": status, "created_at": "2026-01-02 09:00:00",
                              "updated_at": updated}]).astype(object)
    silver.apply_orders(con, order("refunded", "2026-01-05 09:00:00"), "r1")
    silver.apply_orders(con, order("paid", "2026-01-02 09:00:00"), "r2")        # late replay of the old version
    assert con.execute("SELECT order_status FROM silver.orders").fetchone()[0] == "refunded"


def test_settlement_reference_is_normalised(con):
    df = pd.DataFrame([{"settlement_id": "S1", "gateway_ref": "gw-txn123", "merchant_id": "M1", "gross_amount": "10",
                        "fee_amount": "1", "net_amount": "9", "settlement_date": "2026-01-02"}]).astype(object)
    silver.apply_settlements(con, df, "r1")
    assert con.execute("SELECT gateway_ref_norm FROM silver.settlements").fetchone()[0] == "TXN123"


# ---- ingestion --------------------------------------------------------------------------------------
def write_pages(day: date, n_pages: int, per_page: int = 2) -> None:
    folder = config.source_dir() / "gateway" / day.isoformat()
    folder.mkdir(parents=True)
    for p in range(1, n_pages + 1):
        data = [{"txn_id": f"T{p}-{i}", "amount": 1.5} for i in range(per_page)]
        (folder / f"page_{p:03d}.json").write_text(
            json.dumps({"page": p, "next_page": p + 1 if p < n_pages else None, "data": data}), encoding="utf-8")


def test_gateway_client_follows_pagination(data_dir):
    write_pages(date(2026, 1, 2), n_pages=3)
    df = ingest.GatewayClient(sleep=lambda s: None).fetch_day(date(2026, 1, 2))
    assert len(df) == 6 and df["amount"].iloc[0] == "1.5"          # everything arrives as text


def test_gateway_client_retries_transient_failures(data_dir):
    write_pages(date(2026, 1, 2), n_pages=1)
    client = ingest.GatewayClient(fail_first=2, sleep=lambda s: None)      # fails twice, succeeds on the 3rd try
    assert len(client.fetch_day(date(2026, 1, 2))) == 2
    assert client.calls == 3                                                # 2 failures + 1 success (no next page)


def test_gateway_client_gives_up_after_max_retries(data_dir):
    write_pages(date(2026, 1, 2), n_pages=1)
    with pytest.raises(ingest.IngestError):
        ingest.GatewayClient(fail_first=99, sleep=lambda s: None).fetch_day(date(2026, 1, 2))


def test_bronze_partition_is_overwritten_not_appended(data_dir):
    df = pd.DataFrame({"a": ["1", "2"]})
    ingest.write_bronze("gateway", date(2026, 1, 2), df, "run1", "src")
    path = ingest.write_bronze("gateway", date(2026, 1, 2), df.iloc[:1], "run2", "src")
    out = pd.read_parquet(path)
    assert len(out) == 1 and out["_run_id"].iloc[0] == "run2"


# ---- quarantine replay ----------------------------------------------------------------------------------
def test_replay_releases_rows_that_now_pass_and_keeps_the_rest(con, data_dir):
    from payreconcile import replay
    rows = [gw_row(txn_id="T1", order_id="O1", merchant_id="M9"),                    # merchant onboarded later
            gw_row(txn_id="T2", order_id="O2", amount="-5")]                          # still invalid
    for r in rows:
        con.execute("INSERT INTO quarantine.rows VALUES ('r0', DATE '2026-01-02', 'gateway', ?, ?, 'X', 'X', ?, now())",
                    [r["txn_id"], r["order_id"], json.dumps(r)])
    con.execute("""INSERT INTO silver.merchants_scd2 VALUES ('M9', 'New', 'food', 'Pune', 'low', 2.0,
                   DATE '2026-01-01', DATE '9999-12-31', true, 'h', 'seed')""")
    con.close()
    out = replay.replay("gateway", date(2026, 1, 2), run_dbt=False)
    assert out["candidates"] == 2 and out["released"] == 1 and out["still_quarantined"] == 1
    c = duckdb_connect(data_dir)
    assert [r[0] for r in c.execute("SELECT txn_id FROM silver.gateway_txns").fetchall()] == ["T1"]
    left = c.execute("SELECT natural_key, rule_ids FROM quarantine.rows").fetchall()
    assert left == [("T2", "GW-VAL-03")]
    c.close()


def duckdb_connect(data_dir):
    from payreconcile import db
    return db.connect()
