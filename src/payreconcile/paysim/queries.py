"""The SQL behind the PaySim dashboard, in one place so the benchmark times exactly what the dashboard runs.
Every query reads Gold or Silver tables (never the raw file); parameters use DuckDB `?` placeholders."""

from __future__ import annotations

KPIS = """
SELECT count(*) AS transactions, sum(amount) AS total_value, sum(is_fraud) AS fraud_transactions,
       coalesce(sum(amount) FILTER (WHERE is_fraud = 1), 0) AS fraud_amount, sum(is_fraud)::DOUBLE / count(*) AS fraud_rate,
       sum(is_flagged_fraud) AS flagged_transactions FROM gold.fact_transactions"""

DQ_SUMMARY = """
SELECT (SELECT coalesce(sum(failed_count), 0) FROM silver.dq_results WHERE severity = 'ERROR') AS error_failures,
       (SELECT coalesce(sum(failed_count), 0) FROM silver.dq_results WHERE severity = 'WARNING') AS warning_failures,
       (SELECT count(*) FROM silver.dq_quarantine) AS quarantined_rows"""

TYPE_SUMMARY = "SELECT * FROM gold.transaction_type_summary ORDER BY transactions DESC"
HOURLY = "SELECT step, count(*) AS transactions, sum(amount) AS amount, sum(is_fraud) AS fraud FROM gold.fact_transactions GROUP BY step ORDER BY step"
DAILY_TYPE = "SELECT day, type, transactions, total_amount, fraud_transactions, fraud_amount FROM gold.fraud_daily_summary ORDER BY day, type"
TOP_SENDERS = ("SELECT account, transactions, amount_sent, distinct_counterparties FROM gold.account_activity WHERE account_kind = 'customer' "
               "ORDER BY amount_sent DESC LIMIT 20")
TOP_RECEIVERS = ("SELECT account, account_kind, transactions, amount_received, distinct_counterparties FROM gold.account_activity "
                 "ORDER BY amount_received DESC LIMIT 20")
FLAG_PERFORMANCE = "SELECT * FROM gold.fraud_flag_performance ORDER BY scope"
FRAUD_BY_HOUR_OF_DAY = ("SELECT hour_of_day, count(*) AS transactions, sum(is_fraud) AS fraud, sum(is_fraud)::DOUBLE / count(*) AS fraud_rate "
                        "FROM gold.fact_transactions GROUP BY hour_of_day ORDER BY hour_of_day")
FRAUD_AMOUNT_DISTRIBUTION = """
SELECT type, is_fraud, floor(log10(greatest(amount, 1)))::INTEGER AS log10_bucket, count(*) AS transactions
FROM gold.fact_transactions WHERE type IN ('TRANSFER', 'CASH_OUT') GROUP BY ALL ORDER BY type, is_fraud, log10_bucket"""
TOP_FRAUD_ACCOUNTS = ("SELECT * FROM gold.fraud_account_summary ORDER BY fraud_amount_sent + fraud_amount_received DESC LIMIT 25")
BALANCE_QUALITY = "SELECT * FROM gold.balance_quality ORDER BY type, balance_consistency_status"
BALANCE_STATUS_BY_TYPE = """
SELECT type, balance_consistency_status, count(*) AS transactions FROM gold.fact_transactions GROUP BY ALL ORDER BY type"""
LARGEST_DISCREPANCIES = """
SELECT source_row_number, step, type, amount, name_orig, name_dest, origin_balance_error, destination_balance_error, is_fraud
FROM gold.fact_transactions WHERE balance_consistency_status = 'INCONSISTENT'
ORDER BY greatest(abs(coalesce(origin_balance_error, 0)), abs(coalesce(destination_balance_error, 0))) DESC LIMIT 50"""
DQ_RESULTS = "SELECT rule_name, severity, total_checked, failed_count, pass_rate, description FROM silver.dq_results ORDER BY severity, rule_name"
MODEL_METRICS = "SELECT * FROM gold.model_metrics ORDER BY model, split"
MODEL_IMPORTANCE = "SELECT * FROM gold.model_feature_importance"
MODEL_AUDIT = "SELECT * FROM gold.model_feature_audit ORDER BY univariate_roc_auc DESC NULLS LAST"
MODEL_PR_CURVE = "SELECT * FROM gold.model_pr_curve ORDER BY model, recall"
MODEL_RUN = "SELECT * FROM gold.model_run"

# Account investigation: any combination of filters; NULL / empty means "no filter".
ACCOUNT_FILTER = """
SELECT source_row_number, step, day, type, amount, name_orig, old_balance_org, new_balance_org, name_dest, old_balance_dest,
       new_balance_dest, is_fraud, is_flagged_fraud, balance_consistency_status, origin_balance_error, destination_balance_error
FROM gold.fact_transactions
WHERE (? = '' OR name_orig = ?) AND (? = '' OR name_dest = ?) AND (? = '' OR type = ?)
  AND (? = 'any' OR is_fraud = (? = 'fraud')::INTEGER) AND step BETWEEN ? AND ?
ORDER BY step, source_row_number LIMIT 2000"""


def account_filter_params(name_orig: str, name_dest: str, ttype: str, fraud: str, step_lo: int, step_hi: int) -> list:
    return [name_orig, name_orig, name_dest, name_dest, ttype, ttype, fraud, fraud, step_lo, step_hi]


ACCOUNT_HISTORY = """
SELECT source_row_number, step, type, CASE WHEN name_orig = ? THEN 'sent' ELSE 'received' END AS role, amount,
       CASE WHEN name_orig = ? THEN name_dest ELSE name_orig END AS counterparty,
       CASE WHEN name_orig = ? THEN old_balance_org ELSE old_balance_dest END AS balance_before,
       CASE WHEN name_orig = ? THEN new_balance_org ELSE new_balance_dest END AS balance_after, is_fraud, is_flagged_fraud
FROM gold.fact_transactions WHERE name_orig = ? OR name_dest = ? ORDER BY step, source_row_number LIMIT 2000"""

# The parameter-free queries, for the dashboard benchmark.
DASHBOARD_QUERIES: dict[str, str] = {
    "kpis": KPIS, "dq_summary": DQ_SUMMARY, "type_summary": TYPE_SUMMARY, "hourly": HOURLY, "daily_by_type": DAILY_TYPE,
    "top_senders": TOP_SENDERS, "top_receivers": TOP_RECEIVERS, "flag_performance": FLAG_PERFORMANCE,
    "fraud_by_hour_of_day": FRAUD_BY_HOUR_OF_DAY, "fraud_amount_distribution": FRAUD_AMOUNT_DISTRIBUTION,
    "top_fraud_accounts": TOP_FRAUD_ACCOUNTS, "balance_quality": BALANCE_QUALITY, "balance_status_by_type": BALANCE_STATUS_BY_TYPE,
    "largest_discrepancies": LARGEST_DISCREPANCIES, "dq_results": DQ_RESULTS,
}
