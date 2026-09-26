"""PaySim: fraud analysis. isFraud is the ground-truth label; isFlaggedFraud is the source's own rule."""

from __future__ import annotations

import streamlit as st
from ps_common import current, note_if_sample, pct, pq, queries


def render() -> None:
    name = current()
    st.title("PaySim fraud analysis")
    st.caption("`isFraud` is the label. `isFlaggedFraud` is an existing operational rule, scored against the label below.")
    note_if_sample(name)
    t = pq(name, queries.TYPE_SUMMARY)
    f = t[t["fraud_transactions"] > 0]
    a, b, c = st.columns(3)
    with a:
        st.subheader("Fraud count by type")
        st.bar_chart(f.set_index("type")["fraud_transactions"])
    with b:
        st.subheader("Fraud rate by type")
        st.bar_chart(f.set_index("type")["fraud_rate"])
    with c:
        st.subheader("Fraud amount by type")
        st.bar_chart(f.set_index("type")["fraud_amount"])
    st.caption("Fraud occurs only in TRANSFER and CASH_OUT in this data; PAYMENT, CASH_IN and DEBIT have none.")
    st.dataframe(t[["type", "transactions", "fraud_transactions", "fraud_rate", "fraud_amount", "flagged_transactions"]], hide_index=True, width="stretch")

    st.subheader("Fraud over time")
    d = pq(name, queries.DAILY_TYPE)
    daily = d.groupby("day")[["fraud_transactions", "transactions"]].sum()
    daily["fraud_rate"] = daily["fraud_transactions"] / daily["transactions"]
    x, y = st.columns(2)
    x.markdown("**Fraud transactions per day**")
    x.bar_chart(daily["fraud_transactions"])
    y.markdown("**Fraud rate per day**")
    y.line_chart(daily["fraud_rate"])
    y.caption("Late in the simulation almost no legitimate traffic remains, so the daily rate climbs towards 100%: a property of this "
              "dataset, not a fraud surge. Read it together with the transaction counts.")
    h = pq(name, queries.FRAUD_BY_HOUR_OF_DAY)
    st.markdown("**Fraud transactions by hour of day** (simulation hour mod 24)")
    st.bar_chart(h.set_index("hour_of_day")["fraud"])

    st.subheader("Amount distribution: TRANSFER and CASH_OUT")
    dist = pq(name, queries.FRAUD_AMOUNT_DISTRIBUTION)
    dist["amount_band"] = dist["log10_bucket"].map(lambda k: f"1e{k}")
    dist["label"] = dist["is_fraud"].map({0: "not fraud", 1: "fraud"})
    tsel = st.radio("Type", ["TRANSFER", "CASH_OUT"], horizontal=True)
    sub = dist[dist["type"] == tsel]
    share = sub.pivot_table(index="log10_bucket", columns="label", values="transactions", aggfunc="sum").fillna(0)
    share = share / share.sum()
    share.index = [f"1e{k}" for k in share.index]
    st.caption("Share of each class in each order-of-magnitude amount band (each column sums to 100%), so the two classes are comparable "
               "despite the very different counts.")
    st.bar_chart(share)

    st.subheader("Existing flag versus the label")
    fp = pq(name, queries.FLAG_PERFORMANCE)
    st.dataframe(fp, hide_index=True, width="stretch")
    overall = fp[fp["scope"] == "ALL"].iloc[0]
    st.write(f"The rule flagged {int(overall['tp'] + overall['fp']):,} transactions: {int(overall['tp']):,} true positives, "
             f"{int(overall['fp']):,} false positives; it missed {int(overall['fn']):,} fraud transactions (false negatives). "
             f"Precision {pct(overall['precision'], 1)}, recall {pct(overall['recall'], 2)}.")

    st.subheader("Accounts involved in fraud")
    st.dataframe(pq(name, queries.TOP_FRAUD_ACCOUNTS), hide_index=True, width="stretch")
