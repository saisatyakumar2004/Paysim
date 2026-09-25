"""PayReconcile dashboards: finance operations, exceptions, data health, pipeline health, fraud signals.

Run:  streamlit run dashboard/app.py
Reads the warehouse read-only; nothing here can modify data.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from payreconcile import config, db  # noqa: E402

st.set_page_config(page_title="PayReconcile", layout="wide")


@st.cache_data(ttl=30, show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    con = db.connect(read_only=True)
    try:
        return con.execute(sql, list(params)).df()
    finally:
        con.close()


def table_exists(name: str) -> bool:
    schema, table = name.split(".")
    return not q("SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                 (schema, table)).empty


def inr(x: float | None) -> str:
    return "n/a" if x is None or pd.isna(x) else f"₹{x:,.0f}"


if not config.warehouse_path().exists():
    st.error("No warehouse found. Run `python -m payreconcile generate` then `python -m payreconcile backfill ...`.")
    st.stop()

st.title("PayReconcile")
st.caption("Three-way reconciliation of payment gateway, bank settlements and orders. Synthetic data.")
tab_rec, tab_exc, tab_dq, tab_pipe, tab_fraud = st.tabs(
    ["Reconciliation", "Exceptions", "Data health", "Pipeline health", "Fraud signals"])

# ------------------------------------------------------------------------------------------------------
with tab_rec:
    if not table_exists("gold.mart_recon_daily"):
        st.info("Gold layer not built yet.")
    else:
        daily = q("SELECT * FROM gold.mart_recon_daily ORDER BY event_date")
        status = q("""SELECT status, count(*) AS items, round(sum(money_at_risk), 2) AS money_at_risk
                      FROM gold.fact_reconciliation GROUP BY 1 ORDER BY 2 DESC""")
        as_of = q("SELECT as_of_date AS d FROM ops.recon_state WHERE id = 1")["d"].iloc[0]
        dup = q("SELECT coalesce(sum(duplicate_exposure), 0) AS x, count(*) FILTER (WHERE is_duplicate_charge) AS n "
                "FROM gold.fact_reconciliation")
        c = st.columns(5)
        c[0].metric("Money at risk", inr(status["money_at_risk"].sum()))
        c[1].metric("Duplicate-charge exposure", inr(dup["x"].iloc[0]), f"{int(dup['n'].iloc[0])} charges", delta_color="off")
        settled = daily[["matched", "amount_mismatch", "missing_settlement"]].sum()
        c[2].metric("Match rate", f"{settled['matched'] / settled.sum():.2%}" if settled.sum() else "n/a")
        c[3].metric("Fuzzy-matched refs", int(daily["fuzzy_matched"].sum()))
        c[4].metric("As of", str(as_of))
        left, right = st.columns(2)
        with left:
            st.subheader("Money at risk by status")
            st.bar_chart(status[status["money_at_risk"] > 0].set_index("status")["money_at_risk"])
        with right:
            st.subheader("Daily match rate")
            st.line_chart(daily.set_index("event_date")["match_rate"])
        st.subheader("Money at risk per day")
        st.bar_chart(daily.set_index("event_date")["money_at_risk"])
        if table_exists("gold.mart_settlement_lag"):
            lag = q("SELECT txn_date, avg_lag_days, p95_lag_days FROM gold.mart_settlement_lag ORDER BY 1")
            st.subheader("Settlement lag (days)")
            st.line_chart(lag.set_index("txn_date"))
        with st.expander("All statuses"):
            st.dataframe(status, hide_index=True, width="stretch")

# ------------------------------------------------------------------------------------------------------
with tab_exc:
    if not table_exists("gold.fact_reconciliation"):
        st.info("Gold layer not built yet.")
    else:
        statuses = q("SELECT DISTINCT status FROM gold.fact_reconciliation WHERE money_at_risk > 0 OR "
                     "is_duplicate_charge ORDER BY 1")["status"].tolist()
        pick = st.multiselect("Status", statuses, default=[s for s in statuses if s != "MATCHED"][:4])
        merchant = st.text_input("Merchant id (optional)", "")
        if pick:
            marks = ",".join("?" * len(pick))
            sql = (f"SELECT item_id, item_type, status, event_date, merchant_id, expected_amount, settled_amount, "
                   f"diff_amount, money_at_risk, match_method, is_duplicate_charge FROM gold.fact_reconciliation "
                   f"WHERE status IN ({marks})")
            params = list(pick)
            if merchant:
                sql += " AND merchant_id = ?"
                params.append(merchant.strip())
            sql += " ORDER BY money_at_risk DESC LIMIT 500"
            st.dataframe(q(sql, tuple(params)), hide_index=True, width="stretch")
        if table_exists("gold.reconciliation_audit"):
            with st.expander("Status-change history (audit trail)"):
                st.caption("Append-only. Every change of a reconciliation item's status, with the reason: NEW_ITEM, "
                           "INPUT_CHANGE (a late or corrected record arrived), AGED (settlement SLA elapsed), RESOLVED.")
                reasons = q("SELECT reason, count(*) AS n FROM gold.reconciliation_audit GROUP BY 1 ORDER BY 2 DESC")
                st.dataframe(reasons, hide_index=True, width="stretch")
                item = st.text_input("Item id (e.g. TXN:TXN1234567890)", "")
                if item:
                    st.dataframe(q("SELECT run_date, as_of_date, change_kind, reason, old_status, new_status, "
                                   "old_settlement_id, new_settlement_id FROM gold.reconciliation_audit "
                                   "WHERE item_id = ? ORDER BY audit_id", (item.strip(),)), hide_index=True, width="stretch")
                else:
                    st.dataframe(q("SELECT run_date, item_id, change_kind, reason, old_status, new_status "
                                   "FROM gold.reconciliation_audit WHERE change_kind IN ('STATUS_CHANGE', 'CLOSED') "
                                   "ORDER BY audit_id DESC LIMIT 200"), hide_index=True, width="stretch")
        st.subheader("Merchants with the highest exposure")
        st.dataframe(q("SELECT merchant_id, name, category, risk_tier, txns, exceptions, exception_rate, "
                       "round(total_exposure, 2) AS exposure FROM gold.mart_merchant_exceptions "
                       "ORDER BY total_exposure DESC LIMIT 20"), hide_index=True, width="stretch")

# ------------------------------------------------------------------------------------------------------
with tab_dq:
    scores = q("SELECT run_date, dataset, score, status, rows_in, rows_quarantined FROM ops.dq_scores "
               "WHERE run_id IN (SELECT arg_max(run_id, started_at) FROM ops.run_log GROUP BY run_date) "
               "ORDER BY run_date")
    if scores.empty:
        st.info("No runs yet.")
    else:
        latest = scores["run_date"].max()
        st.subheader(f"Latest run: {latest}")
        cur = scores[scores["run_date"] == latest]
        cols = st.columns(len(cur))
        for col, (_, r) in zip(cols, cur.iterrows(), strict=False):
            col.metric(r["dataset"], f"{r['score']:.4f}", f"{r['status']} · {int(r['rows_quarantined'])} quarantined",
                       delta_color="off")
        st.subheader("Data-quality score by dataset")
        st.line_chart(scores.pivot(index="run_date", columns="dataset", values="score"))
        rules = q("""SELECT dataset, rule_id, severity, status, violations, evaluated, round(ratio, 4) AS ratio, action,
                            description FROM ops.dq_results
                     WHERE run_id = (SELECT arg_max(run_id, started_at) FROM ops.run_log)
                       AND status <> 'PASS' ORDER BY status DESC, ratio DESC""")
        st.subheader("Rules that did not pass in the latest run")
        st.dataframe(rules, hide_index=True, width="stretch")
        st.subheader("Quarantine by rule (all runs)")
        qr = q("SELECT dataset, rule_ids, count(*) AS rows FROM quarantine.rows GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 25")
        st.bar_chart(qr.assign(label=qr["dataset"] + " · " + qr["rule_ids"]).set_index("label")["rows"])
        with st.expander("Inspect quarantined rows"):
            ds = st.selectbox("Dataset", ["gateway", "orders", "settlements", "merchants"])
            st.dataframe(q("SELECT run_date, natural_key, rule_ids, raw_json FROM quarantine.rows WHERE dataset = ? "
                           "ORDER BY run_date DESC LIMIT 200", (ds,)), hide_index=True, width="stretch")

# ------------------------------------------------------------------------------------------------------
with tab_pipe:
    runs = q("""SELECT run_date, attempt, status, gate, round(dq_score, 4) AS dq_score, records_read, records_written,
                       records_rejected, round(duration_s, 1) AS seconds, error
                FROM ops.run_log ORDER BY started_at DESC LIMIT 60""")
    if runs.empty:
        st.info("No runs yet.")
    else:
        c = st.columns(4)
        c[0].metric("Runs", len(runs))
        c[1].metric("Succeeded", int(runs["status"].str.startswith("SUCCESS").sum()))
        c[2].metric("Halted by gate", int((runs["status"] == "QUALITY_GATE_FAILED").sum()))
        c[3].metric("Failed", int(runs["status"].str.startswith("FAILED").sum()))
        ordered = runs.sort_values("run_date")
        st.subheader("Duration (s)")
        st.line_chart(ordered.set_index("run_date")["seconds"])
        st.subheader("Records read / rejected")
        st.line_chart(ordered.set_index("run_date")[["records_read", "records_rejected"]])
        st.dataframe(runs, hide_index=True, width="stretch")
        rr = q("SELECT run_date, mode, changes_read, items_evaluated, items_changed, items_total, "
               "round(seconds, 3) AS seconds FROM ops.recon_runs ORDER BY started_at DESC LIMIT 40")
        if not rr.empty:
            st.subheader("Reconciliation: items evaluated vs total")
            st.caption("Incremental runs re-evaluate only what changed (plus items that age past the settlement SLA).")
            st.line_chart(rr.sort_values("run_date").set_index("run_date")[["items_evaluated", "items_total"]])
            st.dataframe(rr, hide_index=True, width="stretch")
        st.subheader("Task timings, latest run")
        st.dataframe(q("SELECT task, status, round(duration_s, 2) AS seconds, rows_in, rows_out, rows_rejected, detail "
                       "FROM ops.task_log WHERE run_id = (SELECT arg_max(run_id, started_at) FROM ops.run_log)"),
                     hide_index=True, width="stretch")

# ------------------------------------------------------------------------------------------------------
with tab_fraud:
    if not table_exists("ml.anomaly_scores"):
        st.info("Run `python -m payreconcile anomalies` to score merchant-days.")
    else:
        flagged = q("""SELECT d AS day, merchant_id, n_payments, n_failed, n_small_failed, n_refunds,
                              round(iforest_score, 3) AS score, iforest_flag, rule_flag
                       FROM ml.anomaly_scores WHERE iforest_flag OR rule_flag ORDER BY score DESC NULLS LAST""")
        m = q("SELECT metric, value FROM ops.model_metrics WHERE model = 'merchant_day_anomaly_iforest' "
              "ORDER BY run_ts DESC LIMIT 2")
        c = st.columns(3)
        c[0].metric("Flagged merchant-days", len(flagged))
        for i, (_, r) in enumerate(m.iterrows(), start=1):
            c[i].metric(f"Isolation Forest {r['metric']} (vs synthetic truth)", f"{r['value']:.2f}")
        st.caption("Features use only trusted (post-quality-gate) data. Baseline rule = any z-score >= 4.")
        st.dataframe(flagged, hide_index=True, width="stretch")
