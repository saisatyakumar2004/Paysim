"""PaySim: account investigation."""

from __future__ import annotations

import streamlit as st
from ps_common import current, inr, note_if_sample, pq, queries


def render() -> None:
    name = current()
    st.title("PaySim account investigation")
    note_if_sample(name)
    lo, hi = pq(name, "SELECT min(step) AS lo, max(step) AS hi FROM gold.fact_transactions").iloc[0]
    c1, c2, c3 = st.columns(3)
    orig = c1.text_input("Origin account (nameOrig)").strip()
    dest = c2.text_input("Destination account (nameDest)").strip()
    ttype = c3.selectbox("Transaction type", ["", "CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"], format_func=lambda x: x or "any")
    c4, c5 = st.columns([1, 2])
    fraud = c4.radio("Fraud status", ["any", "fraud", "not fraud"], horizontal=True)
    steps = c5.slider("Step (simulation hour)", int(lo), int(hi), (int(lo), int(hi)))
    status = {"any": "any", "fraud": "fraud", "not fraud": "clean"}[fraud]
    params = tuple(queries.account_filter_params(orig, dest, ttype, status, steps[0], steps[1]))
    df = pq(name, queries.ACCOUNT_FILTER, params)
    m = st.columns(3)
    m[0].metric("Transactions shown", f"{len(df):,}" + (" (capped at 2,000)" if len(df) >= 2000 else ""))
    m[1].metric("Value shown", inr(df["amount"].sum()))
    m[2].metric("Fraud among them", f"{int(df['is_fraud'].sum()):,}")
    st.dataframe(df, hide_index=True, width="stretch")
    st.download_button("Download this view (CSV)", df.to_csv(index=False).encode("utf-8"), file_name="paysim_transactions.csv", mime="text/csv")

    st.subheader("Account history")
    acct = st.text_input("Account id", orig or dest, help="all transactions where this account is the origin or the destination").strip()
    if acct:
        h = pq(name, queries.ACCOUNT_HISTORY, (acct,) * 6)
        if h.empty:
            st.warning("No transactions for that account.")
        else:
            sent = h.loc[h["role"] == "sent", "amount"].sum()
            received = h.loc[h["role"] == "received", "amount"].sum()
            k = st.columns(4)
            k[0].metric("Transactions", f"{len(h):,}")
            k[1].metric("Sent", inr(sent))
            k[2].metric("Received", inr(received))
            k[3].metric("Fraud involvement", f"{int(h['is_fraud'].sum()):,}")
            st.caption("balance_before / balance_after are the account's own balance in each transaction (0 / 0 means the balance was not recorded).")
            st.dataframe(h, hide_index=True, width="stretch")
            st.line_chart(h.set_index("step")[["balance_after"]])
