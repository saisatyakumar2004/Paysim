"""Pipeline health: runs, gate outcomes, per-stage time and memory, reconciliation work done per run."""

from __future__ import annotations

import streamlit as st
from common import SYNTHETIC_NOTE, guard, q, table_exists


def render() -> None:
    guard()
    st.title("Pipeline health")
    st.caption(f"Operational metadata written by every run (`ops.*`). {SYNTHETIC_NOTE}")
    runs = q("""SELECT run_date, attempt, status, gate, round(dq_score, 4) AS dq_score, records_read, records_written,
                       records_rejected, round(duration_s, 1) AS seconds, error
                FROM ops.run_log ORDER BY started_at DESC LIMIT 90""")
    if runs.empty:
        st.info("No runs yet.")
        return
    c = st.columns(4)
    c[0].metric("Runs shown", len(runs))
    c[1].metric("Succeeded", int(runs["status"].str.startswith("SUCCESS").sum()))
    c[2].metric("Halted by quality gate", int((runs["status"] == "QUALITY_GATE_FAILED").sum()))
    c[3].metric("Failed", int(runs["status"].str.startswith("FAILED").sum()))
    ordered = runs.sort_values("run_date")
    a, b = st.columns(2)
    a.subheader("Run duration (s)")
    a.line_chart(ordered.set_index("run_date")["seconds"])
    b.subheader("Records read / rejected")
    b.line_chart(ordered.set_index("run_date")[["records_read", "records_rejected"]])
    st.dataframe(runs, hide_index=True, width="stretch")

    st.subheader("Stage timing and memory (latest run)")
    st.dataframe(q("""SELECT task AS stage, status, round(duration_s, 2) AS seconds, rows_in, rows_out, rows_rejected,
                             round(peak_rss_mb) AS peak_rss_mb, detail FROM ops.task_log
                      WHERE run_id = (SELECT arg_max(run_id, started_at) FROM ops.run_log)"""), hide_index=True, width="stretch")
    st.caption("peak_rss_mb is the sampled peak resident memory of the process tree (includes the dbt subprocess); it can miss "
               "spikes shorter than the 50 ms sampling interval.")

    st.subheader("Median stage cost across all runs")
    st.dataframe(q("""SELECT task AS stage, count(*) AS runs, round(median(duration_s), 2) AS median_s, round(max(duration_s), 2) AS max_s,
                             round(median(peak_rss_mb)) AS median_peak_rss_mb, round(max(peak_rss_mb)) AS max_peak_rss_mb
                      FROM ops.task_log GROUP BY 1 ORDER BY 3 DESC"""), hide_index=True, width="stretch")

    if table_exists("ops.recon_runs"):
        rr = q("""SELECT run_date, mode, changes_read, items_evaluated, items_changed, items_total, round(seconds, 3) AS seconds
                  FROM ops.recon_runs ORDER BY started_at DESC LIMIT 60""")
        if not rr.empty:
            st.subheader("Reconciliation: items evaluated vs total")
            st.caption("Incremental runs re-evaluate only what changed (plus items that age past the settlement SLA).")
            st.line_chart(rr.sort_values("run_date").set_index("run_date")[["items_evaluated", "items_total"]])
            st.dataframe(rr, hide_index=True, width="stretch")
