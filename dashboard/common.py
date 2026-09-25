"""Shared helpers for the dashboard pages: cached read-only queries, formatting, KPI tooltips."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT / "src", ROOT / "dashboard"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from kpis import AGE_BUCKETS, EXCEPTION_STATUSES, KPIS  # noqa: E402

from payreconcile import config, db  # noqa: E402

EXC_SQL = ", ".join(f"'{s}'" for s in EXCEPTION_STATUSES)


@st.cache_data(ttl=30, show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    con = db.connect(read_only=True)
    try:
        return con.execute(sql, list(params)).df()
    finally:
        con.close()


def table_exists(name: str) -> bool:
    schema, table = name.split(".")
    return not q("SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                 (schema, table)).empty


def has_warehouse() -> bool:
    return config.warehouse_path().exists() and table_exists("gold.fact_reconciliation")


def as_of() -> date | None:
    df = q("SELECT as_of_date FROM ops.recon_state WHERE id = 1")
    return None if df.empty else pd.Timestamp(df["as_of_date"].iloc[0]).date()


def inr(x) -> str:
    return "n/a" if x is None or pd.isna(x) else f"₹{x:,.0f}"


def pct(x) -> str:
    return "n/a" if x is None or pd.isna(x) else f"{x:.2%}"


def kpi_help(key: str) -> str:
    k = KPIS[key]
    return f"{k.definition}\n\nFormula: {k.formula}\n\nMeaning: {k.meaning}\n\nSources: {', '.join(k.sources)}"


def age_bucket(days: int) -> str:
    for lo, hi, label in AGE_BUCKETS:
        if lo <= days <= hi:
            return label
    return AGE_BUCKETS[-1][2]


def bucket_order() -> list[str]:
    return [b[2] for b in AGE_BUCKETS]


def guard() -> None:
    """Stop the page with a helpful message when the platform has not been run yet."""
    if not has_warehouse():
        st.error("No reconciliation data found. Run `python -m payreconcile generate` then "
                 "`python -m payreconcile backfill --start 2026-01-01 --end 2026-01-30`.")
        st.stop()


SYNTHETIC_NOTE = "All data in this dashboard is synthetic (seeded generator); nothing here is a real payment."
