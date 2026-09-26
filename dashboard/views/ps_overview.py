"""PaySim: executive overview."""

from __future__ import annotations

import streamlit as st
from ps_common import current, inr, note_if_sample, pct, pq, queries


def render() -> None:
    name = current()
    st.title("PaySim overview")
    st.caption("Origin account -> transaction -> destination account -> balance changes. PaySim has no orders, gateway or settlements; "
               "this is separate from the PayReconcile money-flow pages.")
    note_if_sample(name)
    k = pq(name, queries.KPIS).iloc[0]
    d = pq(name, queries.DQ_SUMMARY).iloc[0]
    c = st.columns(4)
    c[0].metric("Total transactions", f"{int(k['transactions']):,}")
    c[1].metric("Total transaction value", inr(k["total_value"]))
    c[2].metric("Fraud transactions", f"{int(k['fraud_transactions']):,}")
    c[3].metric("Fraud amount", inr(k["fraud_amount"]))
    c = st.columns(4)
    c[0].metric("Fraud rate", pct(k["fraud_rate"]), help="fraud transactions / all transactions")
    c[1].metric("Flagged fraud (isFlaggedFraud)", f"{int(k['flagged_transactions']):,}")
    c[2].metric("Data-quality failures (ERROR)", f"{int(d['error_failures']):,}",
                help="rows rejected by an ERROR rule; they are quarantined, not loaded")
    c[3].metric("Data-quality warnings", f"{int(d['warning_failures']):,}",
                help="rule failures on rows that were kept (e.g. balance inconsistencies); one row can fail several rules")
    st.caption(f"Quarantined rows: {int(d['quarantined_rows']):,}. Fraud rate is small, so accuracy would be misleading: see the Fraud and Model pages.")
    t = pq(name, queries.TYPE_SUMMARY)
    left, right = st.columns(2)
    with left:
        st.subheader("Transactions by type")
        st.bar_chart(t.set_index("type")["transactions"])
    with right:
        st.subheader("Transaction value by type")
        st.bar_chart(t.set_index("type")["total_amount"])
    st.dataframe(t[["type", "transactions", "total_amount", "fraud_transactions", "fraud_rate"]], hide_index=True, width="stretch")
