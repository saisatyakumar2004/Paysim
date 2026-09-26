"""Money flow: where exactly did the money go missing?

Order -> gateway -> fee -> expected settlement -> bank -> merchant, for every chain, with the exact difference at each step,
the earliest stage that breaks, and the records that prove it. Nothing here is computed in the browser: every number is a
column or an aggregate of gold.fact_money_flow (payreconcile.moneyflow).
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
from common import SYNTHETIC_NOTE, as_of, guard, inr, q

from payreconcile import db, moneyflow
from payreconcile.explain import explain

CATEGORIES = {
    "Missing settlements": ("MISSING_SETTLEMENT",),
    "Amount mismatches": ("GATEWAY_AMOUNT_MISMATCH", "BANK_AMOUNT_MISMATCH", "UNEXPLAINED"),
    "Duplicate money": ("DUPLICATE_PAYMENT", "DUPLICATE_SETTLEMENT"),
    "Fee discrepancies": ("FEE_MISMATCH",),
    "Refund discrepancies": ("REFUND_MISMATCH",),
    "Records with no counterpart": ("PAYMENT_WITHOUT_ORDER", "ORDER_WITHOUT_PAYMENT", "ORPHAN_SETTLEMENT",
                                    "MERCHANT_SETTLEMENT_MISMATCH"),
    "Quarantined / blocked": ("QUARANTINED_SOURCE", "DATA_QUALITY_BLOCK", "INSUFFICIENT_EVIDENCE"),
    "Timing": ("TIMING_DELAY", "DATE_ANOMALY", "PENDING"),
}
STAGE_ORDER = ["ORDER", "GATEWAY", "PROCESSING", "BANK_SETTLEMENT", "MERCHANT_SETTLEMENT", "SOURCE_DATA_QUALITY", "MATCHING"]


@st.cache_data(ttl=30, show_spinner=False)
def _summary() -> dict:
    con = db.connect(read_only=True)
    try:
        return {"summary": moneyflow.summary(con), "headline": moneyflow.headline(con),
                "invariants": moneyflow.check_invariants(con)}
    finally:
        con.close()


def _has_money_flow() -> bool:
    return not q("SELECT 1 FROM gold.fact_money_flow LIMIT 1").empty


def _step(a, b):
    return None if a is None or b is None or pd.isna(a) or pd.isna(b) else round(float(b) - float(a), 2)


def _investigation(flow_id: str) -> None:
    con = db.connect(read_only=True)
    try:
        inv = moneyflow.investigate(con, flow_id)
        if inv is None:
            st.warning("No such chain.")
            return
        r = inv["flow"]
        engine_item = flow_id if flow_id.startswith(("TXN:", "STL:", "ORD:")) else None
        info = explain(con, engine_item, include_bronze=False) if engine_item else None
    finally:
        con.close()

    tone = {"OPEN": st.error, "BLOCKED": st.warning, "EXPLAINED": st.info}.get(r["resolution"], st.success)
    tone(f"**{r['classification']}** at **{r['first_breakpoint']}** ({r['resolution']}). Cause: {r['root_cause']}. "
         f"Evidence level: {r['evidence_level']}. Amount in question: {inr(r['exposure_amount'])}.")
    st.caption(r["evidence_summary"])

    s = {x["stage"]: x for x in inv["stages"]}
    rows = [
        ("ORDER", "Customer / order", s["ORDER"]["amount"], None, s["ORDER"]["note"]),
        ("GATEWAY", "Gateway gross", s["GATEWAY"]["amount"], _step(s["ORDER"]["amount"], s["GATEWAY"]["amount"]),
         f"expected at this stage: {r['expected_gateway']}; gap vs expectation {r['gap_gateway']}"),
        ("FEE", "Fee (schedule)", s["PROCESSING"]["amount"],
         None if s["PROCESSING"]["amount"] is None else -round(float(s["PROCESSING"]["amount"]), 2), s["PROCESSING"]["note"]),
        ("EXPECTED", "Expected settlement", s["EXPECTED"]["amount"],
         None if s["EXPECTED"]["amount"] is None or s["GATEWAY"]["amount"] is None
         else round(s["EXPECTED"]["amount"] - s["GATEWAY"]["amount"], 2), s["EXPECTED"]["note"]),
        ("BANK", "Bank settlement", s["BANK_SETTLEMENT"]["amount"],
         _step(s["EXPECTED"]["amount"], s["BANK_SETTLEMENT"]["amount"]) if s["BANK_SETTLEMENT"]["amount"] is not None
         else r["difference_amount"], s["BANK_SETTLEMENT"]["note"]),
        ("MERCHANT", "Merchant received", None, None, s["MERCHANT_SETTLEMENT"]["note"]),
        ("RESULT", f"Final: {r['resolution']}", None, r["difference_amount"],
         f"bank - expected settlement; first breakpoint {r['first_breakpoint']}"),
    ]
    st.dataframe(pd.DataFrame(rows, columns=["stage", "step", "amount (INR)", "difference from previous stage (INR)", "note"]),
                 hide_index=True, width="stretch")
    b1, b2 = st.columns(2)
    with b1:
        st.markdown("**Where the bank difference comes from** (signed, adds up exactly)")
        st.dataframe(pd.DataFrame(inv["bridge"], columns=["component", "INR"]), hide_index=True, width="stretch")
    with b2:
        st.markdown("**Underlying records**")
        st.dataframe(inv["evidence"], hide_index=True, width="stretch")
        if not len(inv["evidence"]):
            st.caption("A fully reconciled chain keeps its ids only: " + ", ".join(
                f"{k}={r[k]}" for k in ("order_id", "txn_id", "settlement_ids") if r[k]))
    if info and info["current"] is not None:
        with st.expander("Reconciliation engine view (status history and match evidence)"):
            st.write(f"Engine status: **{info['current']['status']}** since {info['current']['status_since']}, "
                     f"match method {info['current']['match_method'] or 'n/a'}")
            st.dataframe(pd.DataFrame(info["audit_history"]), hide_index=True, width="stretch")
            ev = pd.DataFrame(info["match_evidence"])
            if len(ev):
                st.dataframe(ev, hide_index=True, width="stretch")


def render() -> None:
    guard()
    st.title("Money flow")
    if not _has_money_flow():
        st.error("The money-flow model has not been built yet. Run the pipeline (`python -m payreconcile backfill ...`) or "
                 "`python -m payreconcile money-flow --rebuild`.")
        st.stop()
    st.caption(f"Where exactly did the money go missing? Reconciled as of {as_of()}. {SYNTHETIC_NOTE}")
    bundle = _summary()
    s, w = bundle["summary"], bundle["summary"]["waterfall"]
    st.info(bundle["headline"])
    if not bundle["invariants"]["ok"]:
        st.error("A money-conservation check failed: " + ", ".join(k for k, v in bundle["invariants"]["checks"].items() if not v))

    c = st.columns(6)
    c[0].metric("Expected settlement", inr(w["expected_settlement"]),
                help="What should have reached the bank after scheduled fees, over every payment and refund chain.")
    c[1].metric("Reconciled", inr(w["reconciled"]), help="Expected settlement of chains whose bank money agrees (rounding and timing only).")
    c[2].metric("Money at risk", inr(w["at_risk"]), help="Explained + unexplained + blocked. Each chain counted once, under its first breakpoint.")
    c[3].metric("Unexplained", inr(w["unexplained"]), help="Confirmed gap at a known stage with no documented cause in the source data.")
    c[4].metric("Blocked / unverifiable", inr(w["blocked"]), help="Cannot be decided: a record is quarantined, a batch was not loaded, or a candidate cannot be confirmed.")
    c[5].metric("In flight", inr(w["in_flight"]), help="Settlement not yet due (inside the SLA).")

    st.subheader("Waterfall: customer -> gateway -> fees -> expected settlement -> bank -> merchant")
    wf = pd.DataFrame([
        ("Customer / order expected", w["customer_expected"], None, "sum of order amounts carried by a chain"),
        ("Gateway gross captured", w["gateway_gross"], round(w["gateway_gross"] - w["customer_expected"], 2),
         "payments only; includes orders with no payment (negative) and payments with no order (positive)"),
        ("Refunds", w["refunds"], None, "refund chains, money out"),
        ("Fees (scheduled)", -w["expected_fees"], None, "merchant-master fee_pct on the capture date"),
        ("Expected settlement", w["expected_settlement"], None, "gateway gross + refunds - fees"),
        ("Bank actual", w["bank_actual"], round(w["bank_actual"] - w["expected_settlement"], 2),
         "every bank record, including records no payment claims"),
        ("Merchant received", None, None, "NOT AVAILABLE in the source data: the evidence chain stops at the bank"),
        ("Unexplained difference", w["unexplained"], None, "confirmed gaps with no documented cause (absolute, once per chain)"),
    ], columns=["step", "INR", "difference from previous line", "note"])
    st.dataframe(wf, hide_index=True, width="stretch")
    if w["unverifiable_order_side"]:
        st.caption(f"{inr(w['unverifiable_order_side'])} of the gateway total belongs to payments whose order record failed validation, "
                   "so its order side cannot be verified.")
    steps = s["steps_by_resolution"].copy()
    steps.columns = ["resolution", "gateway vs order", "bank amount vs captured", "fee vs schedule", "bank net vs gross - fee", "chains"]
    st.markdown("**The signed differences above, split by what they turned out to be** (each chain counted once)")
    st.dataframe(steps.sort_values("resolution"), hide_index=True, width="stretch")
    st.caption("RECONCILED = rounding within tolerance, IN_FLIGHT = not yet due, TIMING = right money on the wrong date, EXPLAINED = a "
               "mechanism in the data accounts for it (for example a duplicate), BLOCKED = cannot be judged from the data, "
               "OPEN = confirmed gap with no documented cause.")

    left, right = st.columns(2)
    with left:
        st.subheader("Money by first breakpoint (stage)")
        by = s["by_stage"]
        if len(by):
            piv = by.pivot_table(index="stage", columns="resolution", values="exposure", aggfunc="sum").fillna(0)
            piv = piv.reindex([x for x in STAGE_ORDER if x in piv.index])
            st.bar_chart(piv)
            st.dataframe(by, hide_index=True, width="stretch")
    with right:
        st.subheader("Money by root cause (classification)")
        rc = s["by_root_cause"]
        st.bar_chart(rc[rc["exposure"] > 0].groupby("classification")["exposure"].sum().sort_values(ascending=False))
        st.dataframe(rc, hide_index=True, width="stretch")

    st.subheader("Findings")
    counts = q("SELECT classification, count(*) AS chains, coalesce(sum(exposure_amount), 0) AS exposure "
               "FROM gold.fact_money_flow GROUP BY 1")
    cat_rows = [{"category": k, "chains": int(counts[counts["classification"].isin(v)]["chains"].sum()),
                 "money (INR)": round(float(counts[counts["classification"].isin(v)]["exposure"].sum()), 2)}
                for k, v in CATEGORIES.items()]
    st.dataframe(pd.DataFrame(cat_rows), hide_index=True, width="stretch")
    cat = st.radio("Show", list(CATEGORIES), horizontal=True, key="mf_category")
    merchant = st.text_input("Merchant id contains", key="mf_merchant")
    cls = CATEGORIES[cat]
    sql = f"""SELECT flow_id, classification, first_breakpoint AS stage, resolution, merchant_id, event_date, order_amount,
                     gateway_gross, expected_net, bank_net, difference_amount, exposure_amount, root_cause, evidence_level
              FROM gold.fact_money_flow WHERE classification IN ({','.join('?' * len(cls))})
              {"AND merchant_id ILIKE ?" if merchant else ""} ORDER BY exposure_amount DESC, flow_id LIMIT 2000"""
    df = q(sql, tuple(cls) + ((f"%{merchant}%",) if merchant else ()))
    st.caption(f"{len(df):,} chains shown (top 2,000 by amount). Select a row to investigate it.")
    event = st.dataframe(df, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row", key="mf_table")
    st.download_button("Download this list (CSV)", df.to_csv(index=False).encode("utf-8"), file_name="money_flow_findings.csv",
                       mime="text/csv")

    st.subheader("Merchant-level impact")
    mer = q("""SELECT f.merchant_id, m.name, m.risk_tier, count(*) AS chains,
                      round(coalesce(sum(exposure_amount) FILTER (WHERE resolution = 'OPEN'), 0), 2) AS unexplained,
                      round(coalesce(sum(exposure_amount) FILTER (WHERE resolution = 'BLOCKED'), 0), 2) AS blocked,
                      round(coalesce(sum(exposure_amount) FILTER (WHERE resolution = 'EXPLAINED'), 0), 2) AS explained,
                      round(coalesce(sum(exposure_amount), 0), 2) AS at_risk
               FROM gold.fact_money_flow f LEFT JOIN silver.merchants_scd2 m ON m.merchant_id = f.merchant_id AND m.is_current
               WHERE f.merchant_id IS NOT NULL GROUP BY 1, 2, 3 ORDER BY at_risk DESC LIMIT 25""")
    a, b = st.columns([2, 3])
    a.bar_chart(mer.head(12).set_index("merchant_id")[["unexplained", "blocked", "explained"]])
    b.dataframe(mer, hide_index=True, width="stretch")

    st.subheader("Investigate an order or transaction")
    picked = None
    rows = getattr(getattr(event, "selection", None), "rows", []) if event is not None else []
    if rows:
        picked = df.iloc[rows[0]]["flow_id"]
    text = st.text_input("Order id, transaction id or chain id (or select a row above)", picked or st.query_params.get("flow", ""),
                         key="mf_lookup")
    if text:
        key = text.strip()
        hits = q("SELECT flow_id, flow_type, classification, resolution, order_id, txn_id FROM gold.fact_money_flow "
                 "WHERE flow_id = ? OR txn_id = ? OR order_id = ? ORDER BY flow_id", (key, key, key))
        if hits.empty:
            st.warning("No chain matches that id.")
        else:
            if len(hits) > 1:
                st.caption("This id belongs to several chains (for example a duplicate charge on one order).")
                st.dataframe(hits, hide_index=True, width="stretch")
            flow_id = hits["flow_id"].iloc[0] if len(hits) == 1 else st.selectbox("Chain", list(hits["flow_id"]))
            _investigation(flow_id)
