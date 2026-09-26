"""Data quality and the financial-consistency engine: Bronze -> silver.transactions / silver.dq_quarantine / silver.dq_results.

Balance semantics (measured on the source, see README): the origin is always a customer account. PAYMENT, DEBIT, TRANSFER and
CASH_OUT take money out of the origin, CASH_IN puts money in. The destination receives the amount for TRANSFER, CASH_OUT and
DEBIT; for CASH_IN the destination is the agent that hands over cash, so its balance falls. Merchant destinations (`M...`) carry no
balances at all in this data. Where both the opening and closing balance are 0 the account's balances are simply not recorded,
so the equation cannot be tested and the answer is INSUFFICIENT_EVIDENCE, never CONSISTENT.

    origin      expected_new = old + amount   (CASH_IN)     old - amount (all other types)
    destination expected_new = old - amount   (CASH_IN)     old + amount (all other types); NOT_APPLICABLE for merchants

Rows that fail an ERROR rule are not loaded into Silver: they go to silver.dq_quarantine with the rules that rejected them and
their raw values. WARNING rules keep the row and record the rule names in `dq_warning_rules`. Nothing is dropped silently.
"""

from __future__ import annotations

import json
from datetime import datetime

from .common import Dataset, cfg, connect, sql_list

ERROR_RULES = [
    ("null_or_unparseable_fields", "a transaction-critical field is null or cannot be typed", "e_null"),
    ("invalid_step", "step is below 1", "e_step"),
    ("negative_amount", "amount is negative", "e_neg_amount"),
    ("invalid_transaction_type", "type is not one of the known transaction types", "e_type"),
    ("invalid_fraud_flag", "isFraud or isFlaggedFraud is not 0 or 1", "e_flag"),
    ("negative_balance", "an opening or closing balance is negative", "e_neg_balance"),
]


def _typed_sql(bronze: str) -> str:
    return f"""
    CREATE OR REPLACE TEMP TABLE typed AS
    SELECT source_row_number, try_cast(step AS BIGINT) AS step, type, try_cast(amount AS DOUBLE) AS amount,
           "nameOrig" AS name_orig, try_cast("oldbalanceOrg" AS DOUBLE) AS old_org, try_cast("newbalanceOrig" AS DOUBLE) AS new_org,
           "nameDest" AS name_dest, try_cast("oldbalanceDest" AS DOUBLE) AS old_dest, try_cast("newbalanceDest" AS DOUBLE) AS new_dest,
           try_cast("isFraud" AS BIGINT) AS is_fraud, try_cast("isFlaggedFraud" AS BIGINT) AS is_flagged
    FROM read_parquet('{bronze}')"""


def _duplicates_sql() -> str:
    """Later occurrences of an exact duplicate. A cheap 64-bit hash finds candidates; equality is then checked exactly."""
    cols = "step, type, amount, name_orig, old_org, new_org, name_dest, old_dest, new_dest, is_fraud, is_flagged"
    return f"""
    CREATE OR REPLACE TEMP TABLE dup_rows AS
    WITH h AS (SELECT source_row_number, hash({cols}) AS h FROM typed),
         cand AS (SELECT h FROM h GROUP BY h HAVING count(*) > 1),
         rows AS (SELECT t.* FROM typed t JOIN h USING (source_row_number) WHERE h.h IN (SELECT h FROM cand))
    SELECT source_row_number FROM (
        SELECT source_row_number, row_number() OVER (PARTITION BY {cols} ORDER BY source_row_number) AS rn FROM rows)
    WHERE rn > 1"""


def _checked_sql(tol: float, types: list[str]) -> str:
    return f"""
    CREATE OR REPLACE TEMP TABLE checked AS
    WITH flagged AS (
        SELECT t.*,
               (step IS NULL OR type IS NULL OR amount IS NULL OR name_orig IS NULL OR name_dest IS NULL OR old_org IS NULL
                OR new_org IS NULL OR old_dest IS NULL OR new_dest IS NULL OR is_fraud IS NULL OR is_flagged IS NULL) AS e_null,
               coalesce(step < 1, false) AS e_step,
               coalesce(amount < 0, false) AS e_neg_amount,
               coalesce(type NOT IN ({sql_list(types)}), false) AS e_type,
               coalesce(is_fraud NOT IN (0, 1) OR is_flagged NOT IN (0, 1), false) AS e_flag,
               coalesce(old_org < 0 OR new_org < 0 OR old_dest < 0 OR new_dest < 0, false) AS e_neg_balance,
               coalesce(amount = 0, false) AS w_zero_amount,
               d.source_row_number IS NOT NULL AS w_duplicate,
               coalesce(name_orig = name_dest, false) AS w_same_account,
               coalesce(type <> 'CASH_IN' AND amount > 0 AND old_org < amount, false) AS w_overdraw,
               coalesce(is_flagged = 1 AND type <> 'TRANSFER', false) AS w_flag_outside_transfer
        FROM typed t LEFT JOIN dup_rows d USING (source_row_number)),
    err AS (SELECT *, (e_null OR e_step OR e_neg_amount OR e_type OR e_flag OR e_neg_balance) AS any_error FROM flagged),
    bal AS (
        SELECT *,
               CASE WHEN any_error THEN NULL WHEN type = 'CASH_IN' THEN old_org + amount ELSE old_org - amount END AS exp_org,
               CASE WHEN any_error OR name_dest LIKE 'M%' THEN NULL WHEN type = 'CASH_IN' THEN old_dest - amount
                    ELSE old_dest + amount END AS exp_dest,
               CASE WHEN any_error THEN 'INSUFFICIENT_EVIDENCE'
                    WHEN old_org = 0 AND new_org = 0 THEN 'INSUFFICIENT_EVIDENCE'
                    WHEN abs(new_org - (CASE WHEN type = 'CASH_IN' THEN old_org + amount ELSE old_org - amount END)) <= {tol}
                         THEN 'CONSISTENT' ELSE 'INCONSISTENT' END AS o_status,
               CASE WHEN any_error THEN 'INSUFFICIENT_EVIDENCE'
                    WHEN name_dest LIKE 'M%' THEN 'NOT_APPLICABLE'
                    WHEN old_dest = 0 AND new_dest = 0 THEN 'INSUFFICIENT_EVIDENCE'
                    WHEN abs(new_dest - (CASE WHEN type = 'CASH_IN' THEN old_dest - amount ELSE old_dest + amount END)) <= {tol}
                         THEN 'CONSISTENT' ELSE 'INCONSISTENT' END AS d_status
        FROM err)
    SELECT * FROM bal"""


def _silver_sql(hours: int) -> str:
    warn = ("array_to_string(list_filter([CASE WHEN w_zero_amount THEN 'zero_amount' END, CASE WHEN w_duplicate THEN 'duplicate_record' END, "
            "CASE WHEN w_same_account THEN 'origin_equals_destination' END, CASE WHEN w_overdraw THEN 'origin_overdraw' END, "
            "CASE WHEN w_flag_outside_transfer THEN 'flag_outside_transfer' END, "
            "CASE WHEN o_status = 'INCONSISTENT' THEN 'origin_balance_inconsistent' END, "
            "CASE WHEN d_status = 'INCONSISTENT' THEN 'destination_balance_inconsistent' END], x -> x IS NOT NULL), ',')")
    return f"""
    CREATE OR REPLACE TABLE silver.transactions AS
    SELECT source_row_number, step::INTEGER AS step, ((step - 1) // {hours} + 1)::INTEGER AS day, ((step - 1) % {hours})::INTEGER AS hour_of_day,
           type, amount, name_orig, old_org AS old_balance_org, new_org AS new_balance_org, name_dest,
           old_dest AS old_balance_dest, new_dest AS new_balance_dest, is_fraud::INTEGER AS is_fraud, is_flagged::INTEGER AS is_flagged_fraud,
           CASE WHEN name_orig LIKE 'M%' THEN 'merchant' ELSE 'customer' END AS orig_account_kind,
           CASE WHEN name_dest LIKE 'M%' THEN 'merchant' ELSE 'customer' END AS dest_account_kind,
           exp_org AS expected_new_balance_org,
           CASE WHEN o_status IN ('CONSISTENT', 'INCONSISTENT') THEN new_org - exp_org END AS origin_balance_error,
           o_status AS origin_balance_status,
           CASE o_status WHEN 'CONSISTENT' THEN true WHEN 'INCONSISTENT' THEN false END AS origin_balance_consistent,
           exp_dest AS expected_new_balance_dest,
           CASE WHEN d_status IN ('CONSISTENT', 'INCONSISTENT') THEN new_dest - exp_dest END AS destination_balance_error,
           d_status AS destination_balance_status,
           CASE d_status WHEN 'CONSISTENT' THEN true WHEN 'INCONSISTENT' THEN false END AS destination_balance_consistent,
           CASE WHEN o_status = 'INCONSISTENT' OR d_status = 'INCONSISTENT' THEN 'INCONSISTENT'
                WHEN o_status = 'INSUFFICIENT_EVIDENCE' OR d_status = 'INSUFFICIENT_EVIDENCE' THEN 'INSUFFICIENT_EVIDENCE'
                WHEN o_status = 'CONSISTENT' AND d_status IN ('CONSISTENT', 'NOT_APPLICABLE') THEN 'CONSISTENT'
                ELSE 'NOT_APPLICABLE' END AS balance_consistency_status,
           CASE WHEN {warn} = '' THEN 'PASS' ELSE 'WARNING' END AS dq_status,
           nullif({warn}, '') AS dq_warning_rules
    FROM checked WHERE NOT any_error ORDER BY step, source_row_number"""


def _quarantine_sql(bronze: str) -> str:
    reasons = ("array_to_string(list_filter([CASE WHEN c.e_null THEN 'null_or_unparseable_fields' END, CASE WHEN c.e_step THEN 'invalid_step' END, "
               "CASE WHEN c.e_neg_amount THEN 'negative_amount' END, CASE WHEN c.e_type THEN 'invalid_transaction_type' END, "
               "CASE WHEN c.e_flag THEN 'invalid_fraud_flag' END, CASE WHEN c.e_neg_balance THEN 'negative_balance' END], "
               "x -> x IS NOT NULL), ',')")
    return f"""
    CREATE OR REPLACE TABLE silver.dq_quarantine AS
    SELECT b.source_row_number, {reasons} AS dq_error_rules, b.* EXCLUDE (source_row_number)
    FROM read_parquet('{bronze}') b JOIN checked c USING (source_row_number) WHERE c.any_error ORDER BY b.source_row_number"""


def _results(con, schema: dict | None) -> list[dict]:
    """One row per rule: rule_name, description, severity, total_checked, failed_count, pass_rate."""
    exprs: list[tuple[str, str, str, str, str]] = []           # (rule, severity, description, total_expr, failed_expr)
    for rule, desc, col in ERROR_RULES:
        exprs.append((rule, "ERROR", desc, "count(*)", f"count(*) FILTER (WHERE {col})"))
    ok = "NOT any_error"
    exprs += [
        ("zero_amount", "WARNING", "amount is zero", f"count(*) FILTER (WHERE {ok})", f"count(*) FILTER (WHERE {ok} AND w_zero_amount)"),
        ("duplicate_records", "WARNING", "exact duplicate of an earlier row (no natural key exists, so the row is flagged, not removed)",
         f"count(*) FILTER (WHERE {ok})", f"count(*) FILTER (WHERE {ok} AND w_duplicate)"),
        ("origin_equals_destination", "WARNING", "origin and destination are the same account", f"count(*) FILTER (WHERE {ok})",
         f"count(*) FILTER (WHERE {ok} AND w_same_account)"),
        ("origin_overdraw", "WARNING", "a debit moves more money than the origin held (impossible movement)",
         f"count(*) FILTER (WHERE {ok} AND type <> 'CASH_IN' AND amount > 0)", f"count(*) FILTER (WHERE {ok} AND w_overdraw)"),
        ("flag_outside_transfer", "WARNING", "isFlaggedFraud set on a type other than TRANSFER", f"count(*) FILTER (WHERE {ok} AND is_flagged = 1)",
         f"count(*) FILTER (WHERE {ok} AND w_flag_outside_transfer)"),
        ("origin_balance_inconsistent", "WARNING", "origin balance change does not match the amount (evaluable rows only)",
         f"count(*) FILTER (WHERE {ok} AND o_status IN ('CONSISTENT', 'INCONSISTENT'))", f"count(*) FILTER (WHERE {ok} AND o_status = 'INCONSISTENT')"),
        ("destination_balance_inconsistent", "WARNING", "destination balance change does not match the amount (evaluable rows only)",
         f"count(*) FILTER (WHERE {ok} AND d_status IN ('CONSISTENT', 'INCONSISTENT'))", f"count(*) FILTER (WHERE {ok} AND d_status = 'INCONSISTENT')"),
        ("origin_balance_evidence_missing", "INFO", "origin opening and closing balance are both 0: the equation cannot be tested",
         f"count(*) FILTER (WHERE {ok})", f"count(*) FILTER (WHERE {ok} AND o_status = 'INSUFFICIENT_EVIDENCE')"),
        ("destination_balance_evidence_missing", "INFO", "customer destination with both balances 0: the equation cannot be tested",
         f"count(*) FILTER (WHERE {ok} AND d_status <> 'NOT_APPLICABLE')", f"count(*) FILTER (WHERE {ok} AND d_status = 'INSUFFICIENT_EVIDENCE')"),
        ("destination_is_merchant", "INFO", "merchant destination: balances are not recorded for merchants",
         f"count(*) FILTER (WHERE {ok})", f"count(*) FILTER (WHERE {ok} AND d_status = 'NOT_APPLICABLE')"),
    ]
    select = ", ".join(f"{t}, {f}" for _, _, _, t, f in exprs)
    vals = con.execute(f"SELECT {select} FROM checked").fetchone()
    now = datetime.now().isoformat(sep=" ", timespec="seconds")
    out = []
    if schema is not None:
        miss = len(schema["missing"])
        out.append({"rule_name": "required_columns", "description": "every required source column is present", "severity": "ERROR",
                    "total_checked": schema["required"], "failed_count": miss,
                    "pass_rate": 1 - miss / schema["required"], "checked_at": now})
    for i, (rule, sev, desc, _, _) in enumerate(exprs):
        total, failed = int(vals[2 * i]), int(vals[2 * i + 1])
        out.append({"rule_name": rule, "description": desc, "severity": sev, "total_checked": total, "failed_count": failed,
                    "pass_rate": (1 - failed / total) if total else None, "checked_at": now})
    return out


def run_quality(ds: Dataset) -> dict:
    """Bronze -> silver.transactions, silver.dq_quarantine, silver.dq_results (inside ds.db)."""
    c = cfg()
    bronze = ds.bronze.as_posix()
    meta_path = ds.dir / "bronze.meta.json"
    schema = json.loads(meta_path.read_text(encoding="utf-8"))["schema"] if meta_path.exists() else None
    con = connect(ds)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute(_typed_sql(bronze))
        con.execute(_duplicates_sql())
        con.execute(_checked_sql(float(c["balance_tolerance"]), list(c["valid_types"])))
        results = _results(con, schema)
        con.execute(_silver_sql(int(c["hours_per_day"])))
        con.execute(_quarantine_sql(bronze))
        con.execute("CREATE OR REPLACE TABLE silver.dq_results (rule_name VARCHAR, description VARCHAR, severity VARCHAR, "
                    "total_checked BIGINT, failed_count BIGINT, pass_rate DOUBLE, checked_at TIMESTAMP)")
        con.executemany("INSERT INTO silver.dq_results VALUES (?, ?, ?, ?, ?, ?, ?)",
                        [(r["rule_name"], r["description"], r["severity"], r["total_checked"], r["failed_count"], r["pass_rate"],
                          r["checked_at"]) for r in results])
        n_silver = con.execute("SELECT count(*) FROM silver.transactions").fetchone()[0]
        n_quarantine = con.execute("SELECT count(*) FROM silver.dq_quarantine").fetchone()[0]
        n_bronze = con.execute(f"SELECT count(*) FROM read_parquet('{bronze}')").fetchone()[0]
        con.execute("DROP TABLE IF EXISTS typed"), con.execute("DROP TABLE IF EXISTS checked"), con.execute("DROP TABLE IF EXISTS dup_rows")
    finally:
        con.close()
    return {"bronze_rows": int(n_bronze), "silver_rows": int(n_silver), "quarantined_rows": int(n_quarantine), "rules": results,
            "conserved": n_bronze == n_silver + n_quarantine}
