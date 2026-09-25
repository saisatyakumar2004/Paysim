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
    rows_in BIGINT, rows_out BIGINT, rows_rejected BIGINT, detail VARCHAR);
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
