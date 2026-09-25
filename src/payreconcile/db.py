"""DuckDB connection, schemas and helper macros."""

from __future__ import annotations

from pathlib import Path

import duckdb

from . import config

DDL = """
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ml;

CREATE OR REPLACE MACRO pr_amt(x) AS
    try_cast(replace(replace(replace(trim(x), '₹', ''), ',', ''), ' ', '') AS DOUBLE);
CREATE OR REPLACE MACRO pr_ts(x) AS
    coalesce(try_strptime(trim(x), '%Y-%m-%dT%H:%M:%S'),
             try_strptime(trim(x), '%d/%m/%Y %H:%M:%S'),
             try_strptime(trim(x), '%Y-%m-%d %H:%M:%S'));

CREATE TABLE IF NOT EXISTS silver.merchants_scd2 (
    merchant_id VARCHAR, name VARCHAR, category VARCHAR, city VARCHAR, risk_tier VARCHAR, fee_pct DOUBLE,
    valid_from DATE, valid_to DATE, is_current BOOLEAN, row_hash VARCHAR, _run_id VARCHAR);
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id VARCHAR, merchant_id VARCHAR, customer_id VARCHAR, order_amount DOUBLE, order_status VARCHAR,
    created_at TIMESTAMP, updated_at TIMESTAMP, _run_id VARCHAR);
CREATE TABLE IF NOT EXISTS silver.gateway_txns (
    txn_id VARCHAR, order_id VARCHAR, merchant_id VARCHAR, amount DOUBLE, currency VARCHAR, status VARCHAR,
    method VARCHAR, txn_type VARCHAR, refund_of VARCHAR, created_at TIMESTAMP, _run_id VARCHAR);
CREATE TABLE IF NOT EXISTS silver.settlements (
    settlement_id VARCHAR, gateway_ref VARCHAR, gateway_ref_norm VARCHAR, merchant_id VARCHAR,
    gross_amount DOUBLE, fee_amount DOUBLE, net_amount DOUBLE, settlement_date DATE,
    net_consistent BOOLEAN, _run_id VARCHAR);

CREATE TABLE IF NOT EXISTS quarantine.rows (
    run_id VARCHAR, run_date DATE, dataset VARCHAR, natural_key VARCHAR, order_id VARCHAR,
    rule_ids VARCHAR, reason VARCHAR, raw_json VARCHAR, quarantined_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS ops.run_log (
    run_id VARCHAR, run_date DATE, attempt INTEGER, started_at TIMESTAMP, ended_at TIMESTAMP,
    duration_s DOUBLE, status VARCHAR, dq_score DOUBLE, gate VARCHAR, records_read BIGINT,
    records_written BIGINT, records_rejected BIGINT, error VARCHAR);
CREATE TABLE IF NOT EXISTS ops.task_log (
    run_id VARCHAR, run_date DATE, task VARCHAR, status VARCHAR, duration_s DOUBLE,
    rows_in BIGINT, rows_out BIGINT, rows_rejected BIGINT, detail VARCHAR, peak_rss_mb DOUBLE, rss_start_mb DOUBLE);
ALTER TABLE ops.task_log ADD COLUMN IF NOT EXISTS peak_rss_mb DOUBLE;
ALTER TABLE ops.task_log ADD COLUMN IF NOT EXISTS rss_start_mb DOUBLE;
CREATE TABLE IF NOT EXISTS ops.dq_results (
    run_id VARCHAR, run_date DATE, dataset VARCHAR, rule_id VARCHAR, severity VARCHAR, description VARCHAR,
    evaluated BIGINT, violations BIGINT, ratio DOUBLE, status VARCHAR, action VARCHAR);
CREATE TABLE IF NOT EXISTS ops.dq_scores (
    run_id VARCHAR, run_date DATE, dataset VARCHAR, score DOUBLE, status VARCHAR, rows_in BIGINT,
    rows_quarantined BIGINT, loaded BOOLEAN);
CREATE TABLE IF NOT EXISTS ops.batch_stats (
    run_id VARCHAR, run_date DATE, dataset VARCHAR, metric VARCHAR, value DOUBLE, loaded BOOLEAN);
CREATE TABLE IF NOT EXISTS ops.model_metrics (
    run_ts TIMESTAMP, model VARCHAR, metric VARCHAR, value DOUBLE, detail VARCHAR);
CREATE SEQUENCE IF NOT EXISTS ops.change_seq START 1;
CREATE SEQUENCE IF NOT EXISTS gold.audit_seq START 1;

-- Every new/changed Silver row and every quarantine change is recorded here; incremental reconciliation reads it.
CREATE TABLE IF NOT EXISTS ops.change_log (
    seq BIGINT DEFAULT nextval('ops.change_seq'), dataset VARCHAR, key VARCHAR, aux VARCHAR,
    run_id VARCHAR, changed_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS ops.recon_state (
    id INTEGER, last_seq BIGINT, as_of_date DATE, updated_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS ops.recon_runs (
    run_id VARCHAR, run_date DATE, mode VARCHAR, as_of_date DATE, changes_read BIGINT, scope_txns BIGINT,
    scope_settlements BIGINT, scope_orders BIGINT, items_total BIGINT, items_evaluated BIGINT, items_changed BIGINT,
    seconds DOUBLE, started_at TIMESTAMP);

-- Reconciliation state (owned by payreconcile.reconcile; dbt reads these as sources).
CREATE TABLE IF NOT EXISTS gold.fact_reconciliation (
    item_id VARCHAR, item_type VARCHAR, txn_id VARCHAR, settlement_id VARCHAR, order_id VARCHAR, merchant_id VARCHAR,
    event_date DATE, expected_amount DOUBLE, settled_amount DOUBLE, diff_amount DOUBLE, status VARCHAR,
    match_method VARCHAR, is_duplicate_charge BOOLEAN, duplicate_exposure DOUBLE, order_issue VARCHAR,
    money_at_risk DOUBLE, status_since DATE, updated_run_id VARCHAR,
    is_suspected_duplicate BOOLEAN, duplicate_of VARCHAR, duplicate_basis VARCHAR, suspected_duplicate_exposure DOUBLE);
ALTER TABLE gold.fact_reconciliation ADD COLUMN IF NOT EXISTS is_suspected_duplicate BOOLEAN;
ALTER TABLE gold.fact_reconciliation ADD COLUMN IF NOT EXISTS duplicate_of VARCHAR;
ALTER TABLE gold.fact_reconciliation ADD COLUMN IF NOT EXISTS duplicate_basis VARCHAR;
ALTER TABLE gold.fact_reconciliation ADD COLUMN IF NOT EXISTS suspected_duplicate_exposure DOUBLE;
CREATE TABLE IF NOT EXISTS gold.recon_links (
    txn_id VARCHAR, settlement_id VARCHAR, match_method VARCHAR, edit_distance INTEGER);
-- Why two records were (or were not) considered the same transaction: features, score, threshold, decision, reason.
CREATE TABLE IF NOT EXISTS gold.recon_match_evidence (
    txn_id VARCHAR, settlement_id VARCHAR, method VARCHAR, decision VARCHAR, edit_distance INTEGER,
    amount_diff DOUBLE, days_gap INTEGER, merchant_match BOOLEAN, txn_candidates INTEGER, settlement_candidates INTEGER,
    score DOUBLE, threshold VARCHAR, reason VARCHAR, run_id VARCHAR, decided_at TIMESTAMP);
-- Append-only history of every reconciliation status change. Never updated or deleted.
CREATE TABLE IF NOT EXISTS gold.reconciliation_audit (
    audit_id BIGINT DEFAULT nextval('gold.audit_seq'), run_id VARCHAR, run_date DATE, as_of_date DATE,
    item_id VARCHAR, item_type VARCHAR, change_kind VARCHAR, reason VARCHAR,
    old_status VARCHAR, new_status VARCHAR, old_settlement_id VARCHAR, new_settlement_id VARCHAR,
    old_money_at_risk DOUBLE, new_money_at_risk DOUBLE, changed_at TIMESTAMP);

-- Append-only record of every quarantine replay decision (RELEASED / STILL_INVALID / SUPERSEDED).
CREATE TABLE IF NOT EXISTS ops.replay_log (
    replay_id VARCHAR, run_date DATE, dataset VARCHAR, natural_key VARCHAR, outcome VARCHAR,
    old_rule_ids VARCHAR, new_rule_ids VARCHAR, replayed_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS ops.watermarks (dataset VARCHAR PRIMARY KEY, watermark TIMESTAMP);
"""


def connect(path: str | Path | None = None, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    path = Path(path or config.warehouse_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path), read_only=read_only)
    if not read_only:
        con.execute(DDL)
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=4")
    return con
