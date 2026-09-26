"""Data quality: scores, failing rules, quarantine, schema drift and gate decisions."""

from __future__ import annotations

import streamlit as st
from common import SYNTHETIC_NOTE, guard, q


def render() -> None:
    guard()
    st.title("Data quality")
    st.caption(f"Contracts and rules live in `contracts/*.yml` (the rule catalogue). {SYNTHETIC_NOTE}")
    latest_ids = "(SELECT arg_max(run_id, started_at) FROM ops.run_log GROUP BY run_date)"
    scores = q(f"SELECT run_date, dataset, score, status, rows_in, rows_quarantined FROM ops.dq_scores "
               f"WHERE run_id IN {latest_ids} ORDER BY run_date")
    if scores.empty:
        st.info("No runs yet.")
        return
    latest = scores["run_date"].max()
    cur = scores[scores["run_date"] == latest]
    st.subheader(f"Latest run: {latest}")
    cols = st.columns(len(cur))
    for col, (_, r) in zip(cols, cur.iterrows(), strict=False):
        col.metric(r["dataset"], f"{r['score']:.4f}", f"{r['status']} · {int(r['rows_quarantined'])} quarantined", delta_color="off")
    st.caption("Score = severity-weighted mean of (1 - violations / evaluated) per rule; CRITICAL x5, ERROR x3, WARN x1. "
               "Gate: HALT on any FAIL or a score below 0.90; nothing from a halted batch is loaded.")

    left, right = st.columns(2)
    with left:
        st.subheader("Score by dataset")
        st.line_chart(scores.pivot(index="run_date", columns="dataset", values="score"))
    with right:
        st.subheader("Quarantine rate by dataset")
        scores["quarantine_rate"] = scores["rows_quarantined"] / scores["rows_in"].where(scores["rows_in"] > 0)
        st.line_chart(scores.pivot(index="run_date", columns="dataset", values="quarantine_rate"))

    st.subheader("Rules that did not pass in the latest run")
    rules = q("""SELECT dataset, rule_id, severity, status, violations, evaluated, round(ratio, 4) AS ratio, action, description
                  FROM ops.dq_results WHERE run_id = (SELECT arg_max(run_id, started_at) FROM ops.run_log)
                  AND status <> 'PASS' ORDER BY status DESC, ratio DESC""")
    st.dataframe(rules, hide_index=True, width="stretch")

    st.subheader("Schema drift events")
    drift = q(f"""SELECT run_date, dataset, rule_id, violations, description FROM ops.dq_results
                  WHERE rule_id LIKE '%SCH%' AND violations > 0 AND run_id IN {latest_ids} ORDER BY run_date""")
    st.dataframe(drift if not drift.empty else drift, hide_index=True, width="stretch")
    st.caption("SCH-01 missing required column (batch rejected), SCH-02 unexpected column (dropped), SCH-03 renamed column (alias applied).")

    st.subheader("Batch-level checks (volume, freshness, distribution)")
    batch = q(f"""SELECT run_date, dataset, rule_id, status, description FROM ops.dq_results
                  WHERE (rule_id LIKE '%VOL%' OR rule_id LIKE '%FRS%' OR rule_id LIKE '%DST%') AND status <> 'PASS'
                  AND run_id IN {latest_ids} ORDER BY run_date DESC LIMIT 50""")
    st.dataframe(batch, hide_index=True, width="stretch")

    st.subheader("Quarantine by rule (all runs)")
    qr = q("SELECT dataset, rule_ids, count(*) AS rows FROM quarantine.rows GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 25")
    st.bar_chart(qr.assign(label=qr["dataset"] + " · " + qr["rule_ids"]).set_index("label")["rows"])
    with st.expander("Inspect quarantined rows"):
        ds = st.selectbox("Dataset", ["gateway", "orders", "settlements", "merchants"])
        st.dataframe(q("SELECT run_date, natural_key, rule_ids, raw_json FROM quarantine.rows WHERE dataset = ? "
                       "ORDER BY run_date DESC LIMIT 200", (ds,)), hide_index=True, width="stretch")

    st.subheader("Gate decisions")
    gate = q("SELECT coalesce(gate, 'n/a') AS gate, status, count(*) AS runs FROM ops.run_log GROUP BY 1, 2 ORDER BY 3 DESC")
    st.dataframe(gate, hide_index=True, width="stretch")
