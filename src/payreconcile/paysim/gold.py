"""Gold layer: analytical tables built by DuckDB SQL from silver.transactions (nothing is loaded into pandas).

    gold.fact_transactions        one row per accepted transaction: Silver + engineered features + flag outcome (TP/FP/FN/TN)
    gold.fact_fraud               every transaction that is fraud or was flagged, with how the flag performed
    gold.transaction_type_summary volume, value, fraud and balance quality per type
    gold.balance_quality          balance consistency by type and origin/destination status, with the size of the errors
    gold.fraud_daily_summary      per simulated day and type: volume, fraud, flag outcomes
    gold.account_activity         accounts with at least two transactions or any fraud involvement
    gold.fraud_account_summary    accounts that took part in fraud (as origin, as destination, or both) with the fraud chain signal
    gold.fraud_flag_performance   confusion matrix of isFlaggedFraud vs isFraud (overall and per type)
"""

from __future__ import annotations

from .common import Dataset, connect
from .features import classify_columns
from .fraud import flag_performance

FACT_SQL = """
CREATE OR REPLACE TABLE gold.fact_transactions AS
SELECT t.*,
       ln(1 + amount) AS log_amount,
       new_balance_org - old_balance_org AS origin_balance_delta,
       new_balance_dest - old_balance_dest AS destination_balance_delta,
       CASE WHEN old_balance_org > 0 THEN amount / old_balance_org END AS amount_to_origin_balance_ratio,
       CASE WHEN old_balance_dest > 0 THEN amount / old_balance_dest END AS amount_to_destination_balance_ratio,
       (old_balance_org > 0 AND new_balance_org = 0) AS origin_fully_drained,
       ((day - 1) % 7)::INTEGER AS day_of_week,
       CASE WHEN is_fraud = 1 AND is_flagged_fraud = 1 THEN 'TP' WHEN is_fraud = 1 THEN 'FN'
            WHEN is_flagged_fraud = 1 THEN 'FP' ELSE 'TN' END AS flag_outcome
FROM silver.transactions t ORDER BY step, source_row_number"""

FRAUD_SQL = """
CREATE OR REPLACE TABLE gold.fact_fraud AS
SELECT source_row_number, step, day, hour_of_day, type, amount, name_orig, name_dest, old_balance_org, new_balance_org,
       old_balance_dest, new_balance_dest, is_fraud, is_flagged_fraud, flag_outcome, origin_fully_drained,
       origin_balance_status, destination_balance_status, balance_consistency_status
FROM gold.fact_transactions WHERE is_fraud = 1 OR is_flagged_fraud = 1 ORDER BY step, source_row_number"""

TYPE_SQL = """
CREATE OR REPLACE TABLE gold.transaction_type_summary AS
SELECT type, count(*) AS transactions, sum(amount) AS total_amount, avg(amount) AS avg_amount, median(amount) AS median_amount,
       max(amount) AS max_amount, sum(is_fraud) AS fraud_transactions, coalesce(sum(amount) FILTER (WHERE is_fraud = 1), 0) AS fraud_amount,
       sum(is_fraud)::DOUBLE / count(*) AS fraud_rate, sum(is_flagged_fraud) AS flagged_transactions,
       count(*) FILTER (WHERE balance_consistency_status = 'CONSISTENT') AS balance_consistent,
       count(*) FILTER (WHERE balance_consistency_status = 'INCONSISTENT') AS balance_inconsistent,
       count(*) FILTER (WHERE balance_consistency_status = 'INSUFFICIENT_EVIDENCE') AS balance_insufficient_evidence
FROM gold.fact_transactions GROUP BY type ORDER BY transactions DESC"""

BALANCE_SQL = """
CREATE OR REPLACE TABLE gold.balance_quality AS
SELECT type, origin_balance_status, destination_balance_status, balance_consistency_status, count(*) AS transactions,
       sum(amount) AS amount, sum(is_fraud) AS fraud_transactions,
       sum(abs(origin_balance_error)) AS sum_abs_origin_error, max(abs(origin_balance_error)) AS max_abs_origin_error,
       sum(abs(destination_balance_error)) AS sum_abs_destination_error, max(abs(destination_balance_error)) AS max_abs_destination_error
FROM gold.fact_transactions GROUP BY ALL ORDER BY type, balance_consistency_status"""

DAILY_SQL = """
CREATE OR REPLACE TABLE gold.fraud_daily_summary AS
SELECT day, type, count(*) AS transactions, sum(amount) AS total_amount, sum(is_fraud) AS fraud_transactions,
       coalesce(sum(amount) FILTER (WHERE is_fraud = 1), 0) AS fraud_amount, sum(is_flagged_fraud) AS flagged_transactions,
       count(*) FILTER (WHERE flag_outcome = 'TP') AS flag_tp, count(*) FILTER (WHERE flag_outcome = 'FP') AS flag_fp,
       count(*) FILTER (WHERE flag_outcome = 'FN') AS flag_fn
FROM gold.fact_transactions GROUP BY day, type ORDER BY day, type"""

ACCOUNT_SQL = """
CREATE OR REPLACE TABLE gold.account_activity AS
WITH legs AS (
    SELECT name_orig AS account, 'origin' AS role, name_dest AS counterparty, step, amount, is_fraud, is_flagged_fraud FROM gold.fact_transactions
    UNION ALL
    SELECT name_dest, 'destination', name_orig, step, amount, is_fraud, is_flagged_fraud FROM gold.fact_transactions)
SELECT account, CASE WHEN account LIKE 'M%' THEN 'merchant' ELSE 'customer' END AS account_kind, count(*) AS transactions,
       count(*) FILTER (WHERE role = 'origin') AS as_origin, count(*) FILTER (WHERE role = 'destination') AS as_destination,
       coalesce(sum(amount) FILTER (WHERE role = 'origin'), 0) AS amount_sent, coalesce(sum(amount) FILTER (WHERE role = 'destination'), 0) AS amount_received,
       sum(is_fraud) AS fraud_transactions, sum(is_flagged_fraud) AS flagged_transactions, min(step) AS first_step, max(step) AS last_step,
       count(DISTINCT counterparty) AS distinct_counterparties
FROM legs GROUP BY account HAVING count(*) >= 2 OR sum(is_fraud) > 0 ORDER BY transactions DESC, account"""

FRAUD_ACCOUNT_SQL = """
CREATE OR REPLACE TABLE gold.fraud_account_summary AS
WITH f AS (SELECT * FROM gold.fact_transactions WHERE is_fraud = 1),
orig AS (SELECT name_orig AS account, count(*) AS n, sum(amount) AS amt, min(step) AS first_step, max(step) AS last_step FROM f GROUP BY 1),
dest AS (SELECT name_dest AS account, count(*) AS n, sum(amount) AS amt, min(step) AS first_step, max(step) AS last_step FROM f GROUP BY 1),
acc AS (SELECT account FROM orig UNION SELECT account FROM dest)
SELECT a.account, CASE WHEN a.account LIKE 'M%' THEN 'merchant' ELSE 'customer' END AS account_kind,
       coalesce(o.n, 0) AS fraud_as_origin, coalesce(d.n, 0) AS fraud_as_destination, coalesce(o.amt, 0) AS fraud_amount_sent,
       coalesce(d.amt, 0) AS fraud_amount_received, least(o.first_step, d.first_step) AS first_fraud_step,
       greatest(o.last_step, d.last_step) AS last_fraud_step,
       (o.n IS NOT NULL AND d.n IS NOT NULL AND o.first_step >= d.first_step) AS received_then_sent_fraud_funds
FROM acc a LEFT JOIN orig o USING (account) LEFT JOIN dest d USING (account) ORDER BY fraud_amount_received + fraud_amount_sent DESC, a.account"""


def build_gold(ds: Dataset) -> dict:
    """Build every Gold table inside ds.db and return row counts."""
    con = connect(ds)
    counts: dict[str, int] = {}
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute(FACT_SQL)
        classify_columns([r[0] for r in con.execute("DESCRIBE gold.fact_transactions").fetchall()])
        for _, sql in (("fact_fraud", FRAUD_SQL), ("transaction_type_summary", TYPE_SQL), ("balance_quality", BALANCE_SQL),
                          ("fraud_daily_summary", DAILY_SQL), ("account_activity", ACCOUNT_SQL),
                          ("fraud_account_summary", FRAUD_ACCOUNT_SQL)):
            con.execute(sql)
        con.execute("CREATE OR REPLACE TABLE gold.fraud_flag_performance (scope VARCHAR, tp BIGINT, fp BIGINT, tn BIGINT, fn BIGINT, "
                    "precision DOUBLE, recall DOUBLE, f1 DOUBLE, false_positive_rate DOUBLE, prevalence DOUBLE, flagged_rate DOUBLE)")
        con.executemany("INSERT INTO gold.fraud_flag_performance VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        [(r["scope"], r["tp"], r["fp"], r["tn"], r["fn"], r["precision"], r["recall"], r["f1"],
                          r["false_positive_rate"], r["prevalence"], r["flagged_rate"]) for r in flag_performance(con)])
        for t in ("fact_transactions", "fact_fraud", "transaction_type_summary", "balance_quality", "fraud_daily_summary",
                  "account_activity", "fraud_account_summary", "fraud_flag_performance"):
            counts[t] = con.execute(f"SELECT count(*) FROM gold.{t}").fetchone()[0]
    finally:
        con.close()
    return counts

