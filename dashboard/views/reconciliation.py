"""Reconciliation: how the three money trails agree, by status, method and day."""

from __future__ import annotations

import streamlit as st
from common import SYNTHETIC_NOTE, as_of, guard, inr, kpi_help, q


def render() -> None:
    guard()
    st.title("Reconciliation")
    st.caption(f"Three-way match: gateway <-> bank settlement <-> order. Reconciled as of {as_of()}. {SYNTHETIC_NOTE}")

    status = q("""SELECT status, count(*) AS items, round(sum(money_at_risk), 2) AS money_at_risk
                  FROM gold.fact_reconciliation GROUP BY 1 ORDER BY 2 DESC""")
    left, right = st.columns(2)
    with left:
        st.subheader("Items by status")
        st.bar_chart(status.set_index("status")["items"])
    with right:
        st.subheader("Money at risk by status")
        st.bar_chart(status[status["money_at_risk"] > 0].set_index("status")["money_at_risk"],
                     )
    st.caption("Money at risk: " + kpi_help("money_at_risk").split("\n\n")[0])
    st.dataframe(status, hide_index=True, width="stretch")

    st.subheader("How records were matched")
    meth = q("""SELECT coalesce(match_method, 'unmatched') AS method, count(*) AS items FROM gold.fact_reconciliation
                WHERE item_type = 'txn' GROUP BY 1 ORDER BY 2 DESC""")
    a, b = st.columns([1, 2])
    a.dataframe(meth, hide_index=True, width="stretch")
    if q("SELECT 1 FROM information_schema.tables WHERE table_name = 'recon_match_evidence'").shape[0]:
        ev = q("""SELECT method, decision, count(*) AS pairs, round(avg(score), 3) AS avg_score,
                         round(avg(edit_distance), 2) AS avg_edit_distance FROM gold.recon_match_evidence
                  GROUP BY 1, 2 ORDER BY 1, 2""")
        b.dataframe(ev, hide_index=True, width="stretch")
        st.caption("Every match keeps its evidence (features, score, threshold, decision, reason). Ambiguous fuzzy candidates "
                   "are recorded with the reason no match was made. Open the Exceptions Queue to inspect one.")

    st.subheader("Status over time")
    daily = q("""SELECT event_date, status, count(*) AS n FROM gold.fact_reconciliation WHERE item_type = 'txn'
                 GROUP BY 1, 2 ORDER BY 1""")
    st.area_chart(daily.pivot(index="event_date", columns="status", values="n").fillna(0))

    st.subheader("Duplicate charges")
    d = q("""SELECT count(*) FILTER (WHERE is_duplicate_charge) AS confirmed,
                    coalesce(sum(duplicate_exposure), 0) AS confirmed_exposure,
                    count(*) FILTER (WHERE is_suspected_duplicate) AS suspected,
                    coalesce(sum(suspected_duplicate_exposure), 0) AS suspected_exposure FROM gold.fact_reconciliation""").iloc[0]
    c = st.columns(4)
    c[0].metric("Confirmed", f"{int(d['confirmed']):,}", help=kpi_help("duplicate_exposure"))
    c[1].metric("Confirmed exposure", inr(d["confirmed_exposure"]))
    c[2].metric("Suspected (original quarantined)", f"{int(d['suspected']):,}")
    c[3].metric("Suspected exposure", inr(d["suspected_exposure"]))
    st.caption("Confirmed = two trusted payments. Suspected = one trusted payment plus a quarantined attempt on the same order. "
               "They are reported separately and never merged.")
