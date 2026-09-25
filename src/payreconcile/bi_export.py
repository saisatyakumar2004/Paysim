"""Power BI-ready export: a small star schema as CSV and Parquet, plus a manifest.

    python -m payreconcile export-bi --out exports/powerbi

Tables (see docs/powerbi.md for the model, relationships and DAX measures):
  dim_date, dim_merchant (SCD2), dim_payment_method, dim_status,
  fact_transactions, fact_settlements, fact_reconciliation, fact_reconciliation_audit,
  kpi_daily, dq_scores, pipeline_runs

The export reads Silver, the reconciliation state and ops tables directly, so it does not depend on the dbt marts. It does NOT
create a .pbix file: building the report itself must be done in Power BI Desktop.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from . import config, db

EXCEPTION_STATUSES = ("AMOUNT_MISMATCH", "MISSING_SETTLEMENT", "ORPHAN_REFUND", "ORPHAN_SETTLEMENT",
                      "ORDER_WITHOUT_PAYMENT", "SETTLED_NOT_CAPTURED")

STATUS_DIM = [
    # status, category, description, is_exception, is_open, sort
    ("MATCHED", "Resolved", "Gateway payment found in a bank settlement within tolerance", False, False, 1),
    ("AMOUNT_MISMATCH", "Exception", "Settled amount differs from the captured amount beyond tolerance", True, True, 2),
    ("MISSING_SETTLEMENT", "Exception", "No settlement after the settlement SLA elapsed", True, True, 3),
    ("ORPHAN_REFUND", "Exception", "Refund of a transaction that exists nowhere", True, True, 4),
    ("ORPHAN_SETTLEMENT", "Exception", "Bank settlement with no gateway transaction", True, True, 5),
    ("ORDER_WITHOUT_PAYMENT", "Exception", "Paid order with no captured payment", True, True, 6),
    ("SETTLED_NOT_CAPTURED", "Exception", "Bank settled a transaction the gateway did not capture", True, True, 7),
    ("PENDING", "In progress", "Settlement not yet due (inside the SLA)", False, True, 8),
    ("SOURCE_QUARANTINED", "Quality hold", "Bank money for a gateway record that failed validation", False, True, 9),
    ("PAYMENT_QUARANTINED", "Quality hold", "Paid order whose payment failed validation", False, True, 10),
]

QUERIES: dict[str, str] = {
    "dim_date": """
        SELECT cast(d AS DATE) AS date_key, extract(year FROM d)::INTEGER AS year, extract(quarter FROM d)::INTEGER AS quarter,
               extract(month FROM d)::INTEGER AS month, monthname(d) AS month_name, strftime(d, '%Y-%m') AS year_month,
               cast(date_trunc('week', d) AS DATE) AS week_start, dayname(d) AS day_name,
               extract(isodow FROM d)::INTEGER IN (6, 7) AS is_weekend
        FROM generate_series((SELECT least(min(cast(created_at AS DATE)), (SELECT min(settlement_date) FROM silver.settlements))
                              FROM silver.gateway_txns), (SELECT as_of_date FROM ops.recon_state WHERE id = 1) + 3,
                             INTERVAL 1 DAY) AS t(d)""",
    "dim_merchant": """
        SELECT md5(merchant_id || '|' || cast(valid_from AS VARCHAR)) AS merchant_sk, merchant_id, name, category, city,
               risk_tier, fee_pct, valid_from, valid_to, is_current FROM silver.merchants_scd2""",
    "dim_payment_method": """
        SELECT * FROM (VALUES ('upi', 'UPI'), ('card', 'Card'), ('netbanking', 'Net banking'), ('wallet', 'Wallet'))
        AS t(method_key, method_name)""",
    "fact_transactions": """
        SELECT t.txn_id, t.order_id, m.merchant_sk, t.merchant_id, cast(t.created_at AS DATE) AS date_key, t.created_at,
               t.txn_type, t.status, t.method AS method_key, t.amount, t.refund_of,
               (t.status = 'captured' AND t.txn_type = 'payment') AS is_captured_payment
        FROM silver.gateway_txns t
        LEFT JOIN dim_merchant_x m ON m.merchant_id = t.merchant_id AND cast(t.created_at AS DATE) >= m.valid_from
                                   AND cast(t.created_at AS DATE) < m.valid_to""",
    "fact_settlements": """
        SELECT s.settlement_id, s.gateway_ref_norm, l.txn_id, l.match_method, m.merchant_sk, s.merchant_id,
               s.settlement_date AS date_key, s.gross_amount, s.fee_amount, s.net_amount, s.net_consistent,
               s.settlement_date - cast(t.created_at AS DATE) AS lag_days
        FROM silver.settlements s
        LEFT JOIN gold.recon_links l ON l.settlement_id = s.settlement_id
        LEFT JOIN silver.gateway_txns t ON t.txn_id = l.txn_id
        LEFT JOIN dim_merchant_x m ON m.merchant_id = s.merchant_id AND s.settlement_date >= m.valid_from
                                   AND s.settlement_date < m.valid_to""",
    "fact_reconciliation": """
        SELECT r.item_id, r.item_type, r.status, r.txn_id, r.settlement_id, r.order_id, m.merchant_sk, r.merchant_id,
               r.event_date AS date_key, date_diff('day', r.event_date, (SELECT as_of_date FROM ops.recon_state WHERE id = 1)) AS age_days,
               r.status IN ({exc}) AS is_exception, r.status_since, g.method AS method_key, m.risk_tier,
               r.expected_amount, r.settled_amount, r.diff_amount, r.money_at_risk, r.match_method,
               r.is_duplicate_charge, r.duplicate_exposure, r.is_suspected_duplicate, r.suspected_duplicate_exposure,
               r.duplicate_basis, r.order_issue, e.edit_distance AS match_edit_distance, e.score AS match_score
        FROM gold.fact_reconciliation r
        LEFT JOIN silver.gateway_txns g ON g.txn_id = r.txn_id
        LEFT JOIN dim_merchant_x m ON m.merchant_id = r.merchant_id AND r.event_date >= m.valid_from AND r.event_date < m.valid_to
        LEFT JOIN (SELECT txn_id, min(edit_distance) AS edit_distance, max(score) AS score FROM gold.recon_match_evidence
                   WHERE decision = 'LINKED' GROUP BY txn_id) e ON e.txn_id = r.txn_id""",
    "fact_reconciliation_audit": """
        SELECT audit_id, run_date, as_of_date, item_id, item_type, change_kind, reason, old_status, new_status,
               old_settlement_id, new_settlement_id, old_money_at_risk, new_money_at_risk FROM gold.reconciliation_audit""",
    "kpi_daily": """
        SELECT event_date AS date_key,
               count(*) FILTER (WHERE item_type = 'txn') AS txn_items,
               coalesce(sum(expected_amount) FILTER (WHERE item_type = 'txn' AND txn_type_payment), 0) AS processed_amount,
               coalesce(sum(expected_amount) FILTER (WHERE item_type = 'txn' AND status = 'MATCHED'), 0) AS matched_amount,
               count(*) FILTER (WHERE item_type = 'txn' AND status = 'MATCHED') AS matched_items,
               count(*) FILTER (WHERE item_type = 'txn' AND status IN ('AMOUNT_MISMATCH', 'MISSING_SETTLEMENT')) AS unmatched_items,
               count(*) FILTER (WHERE status IN ({exc})) AS exceptions, coalesce(sum(money_at_risk), 0) AS money_at_risk,
               coalesce(sum(duplicate_exposure), 0) AS duplicate_exposure,
               coalesce(sum(suspected_duplicate_exposure), 0) AS suspected_duplicate_exposure
        FROM (SELECT r.*, (r.item_type = 'txn' AND r.expected_amount >= 0) AS txn_type_payment FROM gold.fact_reconciliation r)
        GROUP BY 1 ORDER BY 1""",
    "dq_scores": "SELECT run_date, dataset, score, status, rows_in, rows_quarantined, loaded FROM ops.dq_scores",
    "pipeline_runs": """
        SELECT run_id, run_date, attempt, started_at, duration_s, status, gate, dq_score, records_read, records_written,
               records_rejected FROM ops.run_log""",
}


def export(out: Path, formats: tuple[str, ...] = ("csv", "parquet")) -> dict:
    """Write every table; return the manifest (also saved as manifest.json)."""
    if not config.warehouse_path().exists():
        raise RuntimeError(f"No warehouse at {config.warehouse_path()}: run the pipeline (backfill) before exporting.")
    out.mkdir(parents=True, exist_ok=True)
    con = db.connect(read_only=True)
    try:
        state = con.execute("SELECT as_of_date FROM ops.recon_state WHERE id = 1").fetchone()
        if not state:
            raise RuntimeError("No reconciliation state: run the pipeline (backfill) before exporting.")
        con.execute("CREATE TEMP VIEW dim_merchant_x AS " + QUERIES["dim_merchant"])
        exc = ", ".join(f"'{s}'" for s in EXCEPTION_STATUSES)
        manifest: dict = {"generated_at": datetime.now().isoformat(timespec="seconds"), "as_of_date": str(state[0]),
                          "warehouse": str(config.warehouse_path()), "tables": {},
                          "note": "Synthetic data. Power BI report (.pbix) is NOT included; see docs/powerbi.md."}
        sql_map = dict(QUERIES)
        sql_map["dim_status"] = ("SELECT * FROM (VALUES " + ", ".join(
            f"('{s}', '{c}', '{d}', {str(e).lower()}, {str(o).lower()}, {n})" for s, c, d, e, o, n in STATUS_DIM)
            + ") AS t(status, category, description, is_exception, is_open, sort_order)")
        for name, sql in sql_map.items():
            sql = sql.replace("{exc}", exc)
            n = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]
            cols = [d[0] for d in con.execute(f"SELECT * FROM ({sql}) LIMIT 0").description]
            for fmt in formats:
                target = out / f"{name}.{fmt}"
                if fmt == "csv":
                    con.execute(f"COPY ({sql}) TO '{target.as_posix()}' (HEADER, DELIMITER ',')")
                else:
                    con.execute(f"COPY ({sql}) TO '{target.as_posix()}' (FORMAT parquet)")
            manifest["tables"][name] = {"rows": n, "columns": cols}
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest
    finally:
        con.close()
