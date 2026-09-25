"""Merchant risk: who generates exposure, who leaves exceptions open longest, and how risk attributes changed (SCD2)."""

from __future__ import annotations

import streamlit as st
from common import EXC_SQL, SYNTHETIC_NOTE, as_of, guard, inr, kpi_help, q


def render() -> None:
    guard()
    st.title("Merchant risk")
    st.caption(f"Merchant risk signals from reconciliation outcomes. These are operational signals, not a credit or fraud verdict. {SYNTHETIC_NOTE}")
    asof = as_of()
    df = q(f"""
        SELECT r.merchant_id, m.name, m.category, m.city, m.risk_tier,
               count(*) FILTER (WHERE r.item_type = 'txn') AS txns,
               count(*) FILTER (WHERE r.status IN ({EXC_SQL})) AS exceptions,
               round(count(*) FILTER (WHERE r.status IN ('AMOUNT_MISMATCH', 'MISSING_SETTLEMENT', 'ORPHAN_REFUND')) * 1.0
                     / nullif(count(*) FILTER (WHERE r.item_type = 'txn'), 0), 4) AS exception_rate,
               round(sum(r.money_at_risk), 2) AS money_at_risk,
               round(sum(r.duplicate_exposure), 2) AS dup_confirmed,
               round(sum(r.suspected_duplicate_exposure), 2) AS dup_suspected,
               max(CASE WHEN r.status IN ({EXC_SQL}) THEN date_diff('day', r.event_date, ?::DATE) END) AS oldest_exception_days,
               round(sum(r.money_at_risk) + sum(r.duplicate_exposure), 2) AS exposure
        FROM gold.fact_reconciliation r LEFT JOIN silver.merchants_scd2 m ON m.merchant_id = r.merchant_id AND m.is_current
        WHERE r.merchant_id IS NOT NULL GROUP BY 1, 2, 3, 4, 5""", (asof,))
    tiers = st.multiselect("Risk tier", ["low", "medium", "high"], default=["low", "medium", "high"])
    view = df[df["risk_tier"].isin(tiers)] if tiers else df

    c = st.columns(3)
    c[0].metric("Merchants", f"{len(view):,}")
    c[1].metric("Total exposure", inr(view["exposure"].sum()), help=kpi_help("merchant_exposure"))
    c[2].metric("Longest open exception", f"{int(view['oldest_exception_days'].max() or 0)} d", help=kpi_help("merchant_exception_age"))

    a, b = st.columns(2)
    with a:
        st.subheader("Highest exposure")
        st.bar_chart(view.nlargest(15, "exposure").set_index("merchant_id")["exposure"])
    with b:
        st.subheader("Longest-unresolved exceptions")
        st.bar_chart(view.dropna(subset=["oldest_exception_days"]).nlargest(15, "oldest_exception_days").set_index("merchant_id")["oldest_exception_days"])

    st.subheader("Exposure by risk tier")
    st.dataframe(view.groupby("risk_tier", dropna=False).agg(merchants=("merchant_id", "size"), txns=("txns", "sum"),
                                                              exceptions=("exceptions", "sum"), exposure=("exposure", "sum")).reset_index(),
                 hide_index=True, width="stretch")
    st.caption("If 'high'-tier merchants do not show more exposure than 'low', the tier is not predicting reconciliation trouble.")

    st.subheader("Merchant table")
    st.dataframe(view.sort_values("exposure", ascending=False), hide_index=True, width="stretch")

    st.subheader("Merchant history (SCD Type 2)")
    mid = st.text_input("Merchant id", value=str(view.sort_values("exposure", ascending=False)["merchant_id"].iloc[0]) if len(view) else "")
    if mid:
        st.dataframe(q("SELECT merchant_id, name, category, city, risk_tier, fee_pct, valid_from, valid_to, is_current "
                       "FROM silver.merchants_scd2 WHERE merchant_id = ? ORDER BY valid_from", (mid.strip(),)),
                     hide_index=True, width="stretch")
        st.caption("Each change of risk tier, fee plan or other tracked attribute closes the old row and opens a new one; history is never overwritten.")
