"""PaySim: data quality."""

from __future__ import annotations

import streamlit as st
from ps_common import current, note_if_sample, pq, queries


def render() -> None:
    name = current()
    st.title("PaySim data quality")
    st.caption("ERROR rows are quarantined with their reasons (nothing is dropped silently); WARNING rows are kept and tagged; INFO records "
               "limits of the evidence. `total_checked` differs per rule: it is the population the rule applies to.")
    note_if_sample(name)
    r = pq(name, queries.DQ_RESULTS)
    r["failure_rate"] = 1 - r["pass_rate"]
    sev = st.multiselect("Severity", ["ERROR", "WARNING", "INFO"], default=["ERROR", "WARNING", "INFO"])
    view = r[r["severity"].isin(sev)]
    c = st.columns(3)
    for col, s in zip(c, ["ERROR", "WARNING", "INFO"], strict=True):
        col.metric(f"{s} findings" if s == "INFO" else f"{s} failures", f"{int(r.loc[r['severity'] == s, 'failed_count'].sum()):,}")
    st.dataframe(view[["rule_name", "severity", "total_checked", "failed_count", "failure_rate", "pass_rate", "description"]],
                 hide_index=True, width="stretch")
    worst = view[view["failed_count"] > 0].sort_values("failure_rate", ascending=False)
    if len(worst):
        st.subheader("Failure rate by rule")
        st.bar_chart(worst.set_index("rule_name")["failure_rate"])
    q = pq(name, "SELECT * FROM silver.dq_quarantine LIMIT 200")
    st.subheader("Quarantined rows")
    if q.empty:
        st.success("No row failed an ERROR rule.")
    else:
        st.dataframe(q, hide_index=True, width="stretch")
