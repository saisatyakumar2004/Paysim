"""PaySim: balance integrity."""

from __future__ import annotations

import streamlit as st
from ps_common import current, note_if_sample, pct, pq, queries


def render() -> None:
    name = current()
    st.title("PaySim balance integrity")
    st.caption("Each transaction is tested against the balance equation that fits its type (see README). Where the balances are not recorded "
               "the answer is INSUFFICIENT_EVIDENCE, never CONSISTENT; merchant destinations are NOT_APPLICABLE.")
    note_if_sample(name)
    s = pq(name, queries.BALANCE_STATUS_BY_TYPE)
    total = s.groupby("balance_consistency_status")["transactions"].sum()
    n = total.sum()
    c = st.columns(3)
    for col, status in zip(c, ["CONSISTENT", "INCONSISTENT", "INSUFFICIENT_EVIDENCE"], strict=True):
        col.metric(status.replace("_", " ").title(), f"{int(total.get(status, 0)):,}")
        col.caption(f"{pct(total.get(status, 0) / n, 1)} of transactions")
    st.subheader("Consistency by transaction type")
    st.bar_chart(s.pivot(index="type", columns="balance_consistency_status", values="transactions").fillna(0))
    st.caption("A large INCONSISTENT share is a property of this simulated data (for example, debits larger than the recorded balance), not proof of fraud.")

    q = pq(name, queries.BALANCE_QUALITY)
    st.subheader("Origin and destination detail")
    st.dataframe(q, hide_index=True, width="stretch")
    left, right = st.columns(2)
    o = q.groupby("origin_balance_status")["sum_abs_origin_error"].sum()
    d = q.groupby("destination_balance_status")["sum_abs_destination_error"].sum()
    left.markdown("**Total absolute origin error by status**")
    left.bar_chart(o)
    right.markdown("**Total absolute destination error by status**")
    right.bar_chart(d)

    st.subheader("Largest discrepancies")
    st.dataframe(pq(name, queries.LARGEST_DISCREPANCIES), hide_index=True, width="stretch")
