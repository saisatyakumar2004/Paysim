"""Power BI-ready export: a small star schema as CSV and Parquet, plus a manifest.

    python -m payreconcile export-bi --out exports/powerbi

Tables:
  dim_date, dim_merchant (SCD2), dim_payment_method, dim_status, dim_stage, dim_resolution,
  fact_transactions, fact_settlements, fact_reconciliation, fact_reconciliation_audit,
  fact_money_flow (one row per money chain: every stage amount, the signed gap at each step, first breakpoint, root cause,
  resolution, evidence level, exposure), money_flow_bridge (the same gaps, one row per chain and step, for waterfall visuals),
  money_flow_evidence (the underlying records behind every non-reconciled conclusion),
  kpi_daily, dq_scores, pipeline_runs

Relationships: every fact joins dim_merchant on merchant_sk and dim_date on date_key; fact_money_flow.first_breakpoint joins
dim_stage.stage and fact_money_flow.resolution joins dim_resolution.resolution; money_flow_bridge and money_flow_evidence join
fact_money_flow on flow_id.

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
    "dim_stage": """
        SELECT * FROM (VALUES
            ('NONE', 0, 'No breakpoint: the chain reconciles'),
            ('ORDER', 1, 'Customer / order side'),
            ('GATEWAY', 2, 'Payment gateway capture'),
            ('PROCESSING', 3, 'Fees and deductions between capture and settlement'),
            ('BANK_SETTLEMENT', 4, 'Bank settlement'),
            ('MERCHANT_SETTLEMENT', 5, 'Merchant received (no amount exists in the source data)'),
            ('SOURCE_DATA_QUALITY', 6, 'A record needed for the conclusion is quarantined or its batch was not loaded'),
            ('MATCHING', 7, 'A candidate record exists but cannot be confirmed'))
        AS t(stage, sort_order, description)""",
    "dim_resolution": """
        SELECT * FROM (VALUES
            ('RECONCILED', 0, false, 'Money agrees within tolerance'),
            ('IN_FLIGHT', 1, false, 'Not yet due'),
            ('TIMING', 2, false, 'Right money, wrong date'),
            ('EXPLAINED', 3, true, 'A mechanism in the data accounts for the whole gap'),
            ('BLOCKED', 4, true, 'Cannot be decided from the data'),
            ('OPEN', 5, true, 'Confirmed gap at a known stage with no documented cause (unexplained)'))
        AS t(resolution, sort_order, is_finding, description)""",
    "fact_money_flow": """
        SELECT f.flow_id, f.flow_type, f.order_id, f.txn_id, f.merchant_id, m.merchant_sk, f.event_date AS date_key,
               g.method AS method_key, m.risk_tier, f.settlement_ids, f.bank_reference, f.match_method, f.recon_status,
               f.settlement_date, f.settlement_lag_days, f.n_settlements, f.settlement_pattern, f.order_state,
               f.order_amount, f.expected_gateway, f.gateway_gross, f.fee_pct, f.expected_fee, f.expected_net,
               f.bank_gross, f.bank_fee, f.bank_net, f.merchant_amount, f.merchant_stage,
               f.gap_gateway, f.gap_bank_amount, f.gap_fee, f.gap_bank_net, f.difference_amount,
               f.classification, f.first_breakpoint, f.root_cause, f.resolution, f.evidence_level, f.findings,
               f.exposure_amount, f.tolerated_amount, f.counted_in, f.evidence_summary,
               f.resolution IN ('OPEN', 'BLOCKED', 'EXPLAINED') AS is_finding, f.resolution = 'OPEN' AS is_unexplained,
               date_diff('day', f.event_date, f.as_of_date) AS age_days, f.as_of_date
        FROM gold.fact_money_flow f
        LEFT JOIN silver.gateway_txns g ON g.txn_id = f.txn_id
        LEFT JOIN dim_merchant_x m ON m.merchant_id = f.merchant_id AND f.event_date >= m.valid_from AND f.event_date < m.valid_to""",
    "money_flow_bridge": """
        SELECT flow_id, step_order, step, stage, amount, classification, first_breakpoint, resolution, merchant_id FROM (
            SELECT flow_id, 1 AS step_order, 'GATEWAY_VS_ORDER' AS step, 'GATEWAY' AS stage, gap_gateway AS amount,
                   classification, first_breakpoint, resolution, merchant_id FROM gold.fact_money_flow
            UNION ALL SELECT flow_id, 2, 'BANK_GROSS_VS_CAPTURED', 'BANK_SETTLEMENT', gap_bank_amount, classification,
                   first_breakpoint, resolution, merchant_id FROM gold.fact_money_flow
            UNION ALL SELECT flow_id, 3, 'FEE_VS_SCHEDULE', 'PROCESSING', gap_fee, classification, first_breakpoint,
                   resolution, merchant_id FROM gold.fact_money_flow
            UNION ALL SELECT flow_id, 4, 'BANK_NET_VS_GROSS_MINUS_FEE', 'BANK_SETTLEMENT', gap_bank_net, classification,
                   first_breakpoint, resolution, merchant_id FROM gold.fact_money_flow)
        WHERE amount <> 0""",
    "money_flow_evidence": "SELECT flow_id, evidence_type, record_ref, detail, amount FROM gold.money_flow_evidence",
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
                          "note": "Synthetic data. Power BI report (.pbix) is NOT included; see README.md."}
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
