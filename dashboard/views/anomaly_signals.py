"""Anomaly signals: merchant-day transaction anomalies, with the model compared against a simple baseline."""

from __future__ import annotations

import pandas as pd
import streamlit as st
from common import SYNTHETIC_NOTE, guard, q, table_exists

from payreconcile import anomaly, config


def render() -> None:
    guard()
    st.title("Anomaly signals")
    st.warning("These are **transaction anomaly / merchant risk signals**, not a proven fraud detector. The anomalies used for "
               "evaluation are synthetic and injected by the data generator: the numbers demonstrate the methodology, not "
               "real-world fraud detection capability.")
    st.caption(SYNTHETIC_NOTE)
    if not table_exists("ml.anomaly_scores"):
        st.info("Run `python -m payreconcile anomalies` to score merchant-days.")
        return
    scores = q("""SELECT merchant_id, d AS day, n_payments, n_failed, n_small_failed, n_refunds, mean_amount,
                         round(iforest_score, 3) AS iforest_score, iforest_flag, rule_flag, eligible FROM ml.anomaly_scores""")
    flagged = scores[scores["iforest_flag"] | scores["rule_flag"]].sort_values("iforest_score", ascending=False, na_position="last")
    c = st.columns(3)
    c[0].metric("Eligible merchant-days", f"{int(scores['eligible'].sum()):,}")
    c[1].metric("Flagged by Isolation Forest", f"{int(scores['iforest_flag'].sum()):,}")
    c[2].metric("Flagged by z-score rule (any z >= 4)", f"{int(scores['rule_flag'].sum()):,}")

    tpath = config.truth_dir() / "anomaly.parquet"
    if tpath.exists():
        st.subheader("Method comparison against the injected (synthetic) anomalies")
        full = q("SELECT * FROM ml.anomaly_scores")
        full["d"] = pd.to_datetime(full["d"]).dt.date
        ev = anomaly.evaluate_scores(full, pd.read_parquet(tpath))
        table = pd.DataFrame([{"method": name, **{k: ev[key][k] for k in ("flagged", "tp", "fp", "fn", "precision", "recall", "f1")}}
                              for name, key in (("Isolation Forest", "iforest_flag"), ("z-score rule (baseline)", "rule_flag"))])
        st.dataframe(table, hide_index=True, width="stretch")
        st.caption(f"{ev['true_anomalies_eligible']} injected anomalies among {ev['eligible_merchant_days']:,} eligible merchant-days. "
                   "The simple rule beats the model here; the results are shown as measured, not tuned.")
        st.markdown("**Recall by injected pattern (Isolation Forest)**")
        st.dataframe(pd.DataFrame(ev["iforest_recall_by_pattern"]).T.reset_index(names="pattern"), hide_index=True, width="stretch")

    st.subheader("Flagged merchant-days")
    st.dataframe(flagged, hide_index=True, width="stretch")
    with st.expander("How the features are built (leakage controls)"):
        st.markdown(
            "* **Prediction time:** end of the merchant-day. The score describes that day only.\n"
            "* **Feature cutoff:** a day's baseline (mean/std per merchant) uses **previous days only** (expanding window shifted by one).\n"
            "* **Inputs:** trusted Silver gateway transactions only. No settlement, reconciliation status or label is used.\n"
            "* **Refresh rule:** the model refuses to refresh if the latest run was halted by the quality gate.")
