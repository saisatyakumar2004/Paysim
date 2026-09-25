"""Executive overview: the headline numbers a finance lead needs in 30 seconds."""

from __future__ import annotations

import pandas as pd
import streamlit as st
from common import EXC_SQL, SYNTHETIC_NOTE, age_bucket, as_of, bucket_order, guard, inr, kpi_help, pct, q


def render() -> None:
    guard()
    st.title("Executive overview")
    st.caption(f"Payments reconciliation across gateway, bank settlements and orders. {SYNTHETIC_NOTE}")
    asof = as_of()

    tx = q("""SELECT count(*) FILTER (WHERE status = 'captured' AND txn_type = 'payment') AS txns,
                     coalesce(sum(amount) FILTER (WHERE status = 'captured' AND txn_type = 'payment'), 0) AS processed
              FROM silver.gateway_txns""").iloc[0]
    rc = q(f"""SELECT coalesce(sum(expected_amount) FILTER (WHERE item_type = 'txn' AND status = 'MATCHED'), 0) AS matched_amount,
                      coalesce(sum(money_at_risk), 0) AS money_at_risk,
                      count(*) FILTER (WHERE status IN ({EXC_SQL})) AS exceptions,
                      count(*) FILTER (WHERE item_type = 'txn' AND status = 'MATCHED') AS matched,
                      count(*) FILTER (WHERE item_type = 'txn' AND status IN ('AMOUNT_MISMATCH', 'MISSING_SETTLEMENT')) AS unmatched,
                      coalesce(sum(duplicate_exposure), 0) AS dup, coalesce(sum(suspected_duplicate_exposure), 0) AS sdup
               FROM gold.fact_reconciliation""").iloc[0]
    lag = q("""SELECT avg(s.settlement_date - cast(t.created_at AS DATE)) AS avg_lag,
                      quantile_cont(s.settlement_date - cast(t.created_at AS DATE), 0.95) AS p95_lag
               FROM gold.recon_links l JOIN silver.gateway_txns t ON t.txn_id = l.txn_id
               JOIN silver.settlements s ON s.settlement_id = l.settlement_id WHERE t.txn_type = 'payment'""").iloc[0]
    settled = rc["matched"] + rc["unmatched"]

    c = st.columns(5)
    c[0].metric("Total transactions", f"{int(tx['txns']):,}", help=kpi_help("total_transactions"))
    c[1].metric("Processed amount", inr(tx["processed"]), help=kpi_help("processed_amount"))
    c[2].metric("Matched amount", inr(rc["matched_amount"]), help=kpi_help("matched_amount"))
    c[3].metric("Money at risk", inr(rc["money_at_risk"]), help=kpi_help("money_at_risk"))
    c[4].metric("Match rate", pct(rc["matched"] / settled if settled else None), help=kpi_help("match_rate"))
    c = st.columns(5)
    c[0].metric("Open exceptions", f"{int(rc['exceptions']):,}", help=kpi_help("exception_count"))
    c[1].metric("Avg settlement lag", f"{lag['avg_lag']:.2f} d" if pd.notna(lag["avg_lag"]) else "n/a", help=kpi_help("settlement_lag"))
    c[2].metric("p95 settlement lag", f"{lag['p95_lag']:.0f} d" if pd.notna(lag["p95_lag"]) else "n/a", help=kpi_help("settlement_lag"))
    c[3].metric("Duplicate-charge exposure (confirmed)", inr(rc["dup"]), help=kpi_help("duplicate_exposure"))
    c[4].metric("Duplicate-charge exposure (suspected)", inr(rc["sdup"]), help=kpi_help("duplicate_exposure"))
    st.caption(f"Reconciled as of {asof}. Hover a metric for its definition, formula and source tables.")

    left, right = st.columns(2)
    with left:
        st.subheader("Money at risk by exception type")
        by = q("SELECT status, round(sum(money_at_risk), 2) AS money_at_risk FROM gold.fact_reconciliation "
               "WHERE money_at_risk > 0 GROUP BY 1 ORDER BY 2 DESC")
        st.bar_chart(by.set_index("status")["money_at_risk"])
    with right:
        st.subheader("Open exceptions by age")
        ex = q(f"SELECT event_date, money_at_risk FROM gold.fact_reconciliation WHERE status IN ({EXC_SQL})")
        if ex.empty or asof is None:
            st.info("No open exceptions.")
        else:
            ex["bucket"] = [age_bucket((pd.Timestamp(asof) - pd.Timestamp(d)).days) for d in ex["event_date"]]
            agg = ex.groupby("bucket").agg(items=("money_at_risk", "size"), money=("money_at_risk", "sum")).reindex(bucket_order()).fillna(0)
            st.bar_chart(agg["items"])
            st.caption("Number of open exceptions per age bucket (age = as-of date minus event date).")

    st.subheader("Daily reconciliation scorecard")
    daily = q("""SELECT event_date, count(*) FILTER (WHERE item_type = 'txn') AS txns,
                        round(count(*) FILTER (WHERE status = 'MATCHED') * 1.0 / nullif(count(*) FILTER (
                              WHERE item_type = 'txn' AND status NOT IN ('PENDING', 'ORPHAN_REFUND')), 0), 4) AS match_rate,
                        round(sum(money_at_risk), 2) AS money_at_risk
                 FROM gold.fact_reconciliation GROUP BY 1 ORDER BY 1""")
    a, b = st.columns(2)
    a.line_chart(daily.set_index("event_date")["match_rate"])
    b.bar_chart(daily.set_index("event_date")["money_at_risk"])

    st.subheader("Merchants with the highest exposure")
    top = q("SELECT merchant_id, name, category, risk_tier, txns, exceptions, round(total_exposure, 2) AS exposure "
            "FROM gold.mart_merchant_exceptions ORDER BY total_exposure DESC LIMIT 8") \
        if q("SELECT 1 FROM information_schema.tables WHERE table_name = 'mart_merchant_exceptions'").shape[0] else pd.DataFrame()
    if top.empty:
        st.info("Run the dbt build to populate merchant marts.")
    else:
        st.dataframe(top, hide_index=True, width="stretch")
    st.caption("Pipeline last run: see the Pipeline Health page.")
