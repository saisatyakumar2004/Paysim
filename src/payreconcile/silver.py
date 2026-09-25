"""Silver layer: typed, standardised, de-duplicated tables built only from rows that passed data quality.

All loads are idempotent upserts: running the same batch twice leaves Silver unchanged.
"""

from __future__ import annotations

from datetime import date

import pandas as pd


def apply_gateway(con, good: pd.DataFrame, run_id: str) -> int:
    if good.empty:
        return 0
    con.register("stg_raw", good)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stg AS
        SELECT trim(txn_id) AS txn_id, nullif(trim(order_id), '') AS order_id, trim(merchant_id) AS merchant_id,
               pr_amt(amount) AS amount, upper(trim(currency)) AS currency, lower(trim(status)) AS status,
               lower(trim(method)) AS method, lower(trim(txn_type)) AS txn_type,
               nullif(trim(refund_of), '') AS refund_of, pr_ts(created_at) AS created_at, ? AS _run_id
        FROM stg_raw""", [run_id])
    con.execute("DELETE FROM silver.gateway_txns WHERE txn_id IN (SELECT txn_id FROM stg)")
    con.execute("INSERT INTO silver.gateway_txns SELECT * FROM stg")
    n = con.execute("SELECT count(*) FROM stg").fetchone()[0]
    con.unregister("stg_raw")
    return n


def apply_settlements(con, good: pd.DataFrame, run_id: str) -> int:
    if good.empty:
        return 0
    con.register("stg_raw", good)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stg AS
        SELECT trim(settlement_id) AS settlement_id, trim(gateway_ref) AS gateway_ref,
               upper(regexp_replace(trim(gateway_ref), '^(?i:GW-)', '')) AS gateway_ref_norm,
               trim(merchant_id) AS merchant_id, pr_amt(gross_amount) AS gross_amount,
               pr_amt(fee_amount) AS fee_amount, pr_amt(net_amount) AS net_amount,
               try_cast(trim(settlement_date) AS DATE) AS settlement_date,
               abs(pr_amt(net_amount) - (pr_amt(gross_amount) - pr_amt(fee_amount))) <= 0.01 AS net_consistent,
               ? AS _run_id
        FROM stg_raw""", [run_id])
    con.execute("DELETE FROM silver.settlements WHERE settlement_id IN (SELECT settlement_id FROM stg)")
    con.execute("INSERT INTO silver.settlements SELECT * FROM stg")
    n = con.execute("SELECT count(*) FROM stg").fetchone()[0]
    con.unregister("stg_raw")
    return n


def apply_orders(con, good: pd.DataFrame, run_id: str) -> int:
    """Change-log rows -> latest version per order. An older version never overwrites a newer one."""
    if good.empty:
        return 0
    con.register("stg_raw", good)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stg AS
        SELECT trim(order_id) AS order_id, trim(merchant_id) AS merchant_id, trim(customer_id) AS customer_id,
               pr_amt(order_amount) AS order_amount, lower(trim(order_status)) AS order_status,
               pr_ts(created_at) AS created_at, pr_ts(updated_at) AS updated_at, ? AS _run_id
        FROM stg_raw
        QUALIFY row_number() OVER (PARTITION BY trim(order_id) ORDER BY pr_ts(updated_at) DESC) = 1""", [run_id])
    con.execute("""DELETE FROM silver.orders o USING stg s
                   WHERE o.order_id = s.order_id AND o.updated_at <= s.updated_at""")
    con.execute("""INSERT INTO silver.orders SELECT * FROM stg s
                   WHERE NOT EXISTS (SELECT 1 FROM silver.orders o WHERE o.order_id = s.order_id)""")
    n = con.execute("SELECT count(*) FROM stg").fetchone()[0]
    con.unregister("stg_raw")
    return n


def apply_merchants_scd2(con, good: pd.DataFrame, snapshot_date: date, run_id: str) -> tuple[int, int]:
    """SCD Type 2. Returns (new_versions_inserted, versions_closed).

    A tracked-attribute change closes the current row (valid_to = snapshot date, is_current = false) and
    inserts a new current row. A snapshot older than the current version is ignored, so replays are safe.
    """
    if good.empty:
        return 0, 0
    first_load = con.execute("SELECT count(*) FROM silver.merchants_scd2").fetchone()[0] == 0
    con.register("stg_raw", good)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE stg AS
        SELECT trim(merchant_id) AS merchant_id, trim(name) AS name, trim(category) AS category, trim(city) AS city,
               lower(trim(risk_tier)) AS risk_tier, pr_amt(fee_pct) AS fee_pct,
               md5(concat_ws('|', trim(name), trim(category), trim(city), lower(trim(risk_tier)), pr_amt(fee_pct))) AS row_hash
        FROM stg_raw""")
    snap = snapshot_date.isoformat()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE changed AS
        SELECT s.merchant_id FROM stg s JOIN silver.merchants_scd2 c
          ON c.merchant_id = s.merchant_id AND c.is_current
        WHERE c.row_hash <> s.row_hash AND DATE '{snap}' > c.valid_from""")
    closed = con.execute("SELECT count(*) FROM changed").fetchone()[0]
    con.execute(f"""UPDATE silver.merchants_scd2 SET valid_to = DATE '{snap}', is_current = false
                    WHERE is_current AND merchant_id IN (SELECT merchant_id FROM changed)""")
    valid_from = "DATE '1970-01-01'" if first_load else f"DATE '{snap}'"
    con.execute(f"""
        INSERT INTO silver.merchants_scd2
        SELECT s.merchant_id, s.name, s.category, s.city, s.risk_tier, s.fee_pct,
               {valid_from}, DATE '9999-12-31', true, s.row_hash, ?
        FROM stg s
        WHERE s.merchant_id IN (SELECT merchant_id FROM changed)
           OR NOT EXISTS (SELECT 1 FROM silver.merchants_scd2 c WHERE c.merchant_id = s.merchant_id)""", [run_id])
    inserted = con.execute(
        "SELECT count(*) FROM silver.merchants_scd2 WHERE _run_id = ?", [run_id]).fetchone()[0]
    con.unregister("stg_raw")
    return inserted, closed
