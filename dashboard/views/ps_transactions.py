"""PaySim: transaction analysis."""

from __future__ import annotations

import streamlit as st
from ps_common import current, note_if_sample, pq, queries


def render() -> None:
    name = current()
    st.title("PaySim transaction analysis")
    note_if_sample(name)
    t = pq(name, queries.TYPE_SUMMARY)
    a, b = st.columns(2)
    with a:
        st.subheader("Volume by type")
        st.bar_chart(t.set_index("type")["transactions"])
    with b:
        st.subheader("Amount by type")
        st.bar_chart(t.set_index("type")["total_amount"])
    st.dataframe(t[["type", "transactions", "total_amount", "avg_amount", "median_amount", "max_amount"]], hide_index=True, width="stretch")

    st.subheader("Trend")
    grain = st.radio("Grain", ["day", "hour (step)"], horizontal=True)
    if grain == "day":
        d = pq(name, queries.DAILY_TYPE)
        st.caption("Transactions per simulated day, stacked by type.")
        st.bar_chart(d.pivot(index="day", columns="type", values="transactions").fillna(0))
    else:
        h = pq(name, queries.HOURLY)
        st.caption("Transactions per simulation hour (`step`). Volume falls sharply after the first days in this dataset.")
        st.line_chart(h.set_index("step")[["transactions"]])

    st.subheader("Account activity")
    st.caption("Accounts with at least two transactions or any fraud involvement (gold.account_activity).")
    x, y = st.columns(2)
    x.markdown("**Largest senders (customer accounts)**")
    x.dataframe(pq(name, queries.TOP_SENDERS), hide_index=True, width="stretch")
    y.markdown("**Largest receivers**")
    y.dataframe(pq(name, queries.TOP_RECEIVERS), hide_index=True, width="stretch")
