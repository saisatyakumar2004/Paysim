"""Exceptions queue: filterable work list with a full "explain" drill-down back to the source records."""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import streamlit as st
from common import SYNTHETIC_NOTE, as_of, guard, inr, q

from payreconcile import db
from payreconcile.explain import explain

FLAGS = {"Confirmed duplicate charge": "r.is_duplicate_charge",
         "Suspected duplicate charge": "r.is_suspected_duplicate",
         "Payment without order": "r.order_issue = 'NO_ORDER'",
         "Amount differs from order": "r.order_issue = 'AMOUNT_DIFF'"}


def build_query(statuses, flags, merchant, dates, tiers, methods, amount_range, age_range, asof) -> tuple[str, list]:
    """Assemble the exceptions query and its parameters from the filter values (pure function; unit-tested)."""
    include = []
    params: list = []
    if statuses:
        include.append(f"r.status IN ({','.join('?' * len(statuses))})")
        params += list(statuses)
    for f in flags:
        include.append(FLAGS[f])
    where = [f"({' OR '.join(include)})"] if include else ["false"]
    if merchant:
        where.append("(r.merchant_id ILIKE ? OR m.name ILIKE ?)")
        params += [f"%{merchant}%", f"%{merchant}%"]
    if dates and len(dates) == 2:
        where.append("r.event_date BETWEEN ? AND ?")
        params += [dates[0], dates[1]]
    if tiers:
        where.append(f"m.risk_tier IN ({','.join('?' * len(tiers))})")
        params += list(tiers)
    if methods:
        where.append(f"g.method IN ({','.join('?' * len(methods))})")
        params += list(methods)
    where.append("coalesce(abs(r.expected_amount), abs(r.settled_amount), 0) BETWEEN ? AND ?")
    params += [amount_range[0], amount_range[1]]
    where.append("(date_diff('day', r.event_date, ?::DATE)) BETWEEN ? AND ?")
    params += [asof, age_range[0], age_range[1]]
    sql = f"""
        SELECT r.item_id, r.item_type, r.status, r.event_date, date_diff('day', r.event_date, ?::DATE) AS age_days,
               r.merchant_id, m.name AS merchant, m.risk_tier, g.method AS payment_method,
               coalesce(abs(r.expected_amount), abs(r.settled_amount)) AS amount, r.diff_amount, r.money_at_risk,
               r.is_duplicate_charge AS dup_confirmed, r.is_suspected_duplicate AS dup_suspected, r.order_issue,
               r.match_method, r.txn_id, r.settlement_id, r.order_id
        FROM gold.fact_reconciliation r
        LEFT JOIN silver.merchants_scd2 m ON m.merchant_id = r.merchant_id AND m.is_current
        LEFT JOIN silver.gateway_txns g ON g.txn_id = r.txn_id
        WHERE {' AND '.join(where)}
        ORDER BY r.money_at_risk + r.duplicate_exposure + r.suspected_duplicate_exposure DESC, r.event_date
        LIMIT 5000"""
    return sql, [asof, *params]


def render() -> None:
    guard()
    st.title("Exceptions queue")
    st.caption(f"Everything that needs a human decision, with traceability back to the source records. {SYNTHETIC_NOTE}")
    asof = as_of()
    bounds = q("SELECT min(event_date) AS lo, max(event_date) AS hi, max(coalesce(abs(expected_amount), abs(settled_amount), 0)) AS mx, "
               "max(date_diff('day', event_date, ?::DATE)) AS age FROM gold.fact_reconciliation", (asof,)).iloc[0]
    lo, hi = pd.Timestamp(bounds["lo"]).date(), pd.Timestamp(bounds["hi"]).date()
    mx, mx_age = float(bounds["mx"] or 0), int(bounds["age"] or 0)

    c1, c2, c3 = st.columns(3)
    statuses = c1.multiselect("Status", ["AMOUNT_MISMATCH", "MISSING_SETTLEMENT", "ORPHAN_REFUND", "ORPHAN_SETTLEMENT",
                                         "ORDER_WITHOUT_PAYMENT", "SETTLED_NOT_CAPTURED"],
                              default=["AMOUNT_MISMATCH", "MISSING_SETTLEMENT", "ORPHAN_REFUND", "ORPHAN_SETTLEMENT",
                                       "ORDER_WITHOUT_PAYMENT", "SETTLED_NOT_CAPTURED"])
    flags = c2.multiselect("Also include flags", list(FLAGS), default=["Confirmed duplicate charge", "Suspected duplicate charge"])
    merchant = c3.text_input("Merchant (id or name contains)")
    c4, c5, c6 = st.columns(3)
    dates = c4.date_input("Event date range", (lo, hi), min_value=lo, max_value=hi)
    tiers = c5.multiselect("Merchant risk tier", ["low", "medium", "high"])
    methods = c6.multiselect("Payment method", ["upi", "card", "netbanking", "wallet"])
    c7, c8 = st.columns(2)
    amount_range = c7.slider("Amount (INR)", 0.0, max(mx, 1.0), (0.0, max(mx, 1.0)))
    age_range = c8.slider("Age (days)", 0, max(mx_age, 1), (0, max(mx_age, 1)))
    if methods:
        st.caption("Payment method is known for gateway-side items only; bank-only and order-only items are excluded by this filter.")

    sql, params = build_query(statuses, flags, merchant, dates if isinstance(dates, tuple) else (lo, hi), tiers, methods,
                              amount_range, age_range, asof)
    df = q(sql, tuple(params))
    m1, m2, m3 = st.columns(3)
    m1.metric("Exceptions shown", f"{len(df):,}" + (" (capped at 5,000)" if len(df) >= 5000 else ""))
    m2.metric("Money at risk (shown)", inr(df["money_at_risk"].sum()))
    m3.metric("Oldest (days)", int(df["age_days"].max()) if len(df) else 0)
    event = st.dataframe(df, hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row", key="exc_table")
    st.download_button("Download filtered exceptions (CSV)", df.to_csv(index=False).encode("utf-8"),
                       file_name=f"exceptions_{date.today().isoformat()}.csv", mime="text/csv")

    st.subheader("Explain an exception")
    selected = None
    rows = getattr(getattr(event, "selection", None), "rows", []) if event is not None else []
    if rows:
        selected = df.iloc[rows[0]]["item_id"]
    item_id = st.text_input("Item id (select a row above, or paste e.g. TXN:TXN1234567890)",
                            selected or st.query_params.get("item", ""))
    if item_id:
        con = db.connect(read_only=True)
        try:
            info = explain(con, item_id.strip())
        finally:
            con.close()
        if info["current"] is None and not info["audit_history"]:
            st.warning("No such item.")
            return
        cur = info["current"]
        if cur:
            st.markdown(f"**{cur['status']}** since {cur['status_since']} · money at risk {inr(cur['money_at_risk'])} · "
                        f"match method: {cur['match_method'] or 'n/a'}")
            if cur.get("duplicate_basis"):
                st.info(f"Duplicate basis: {cur['duplicate_basis']} (related record: {cur['duplicate_of']})")
        else:
            st.markdown("This item is **closed** (resolved): see the audit history.")
        t1, t2, t3, t4, t5 = st.tabs(["Why matched / not matched", "Status history", "Source records", "Quarantine", "Bronze"])
        with t1:
            ev = pd.DataFrame(info["match_evidence"])
            if ev.empty:
                st.write("No match evidence: nothing was linked to this item.")
            else:
                st.dataframe(ev, hide_index=True, width="stretch")
                st.caption("score = 1 - edit_distance / (max_edit_distance + 1). The decision is the uniqueness rule: a fuzzy pair is "
                           "linked only if it is the sole candidate from both sides.")
        with t2:
            st.dataframe(pd.DataFrame(info["audit_history"]), hide_index=True, width="stretch")
        with t3:
            for name, rows_ in info["silver_records"].items():
                st.markdown(f"**silver.{name}**")
                st.dataframe(pd.DataFrame(rows_), hide_index=True, width="stretch")
        with t4:
            qrows = pd.DataFrame(info["quarantine_rows"])
            if qrows.empty:
                st.write("No quarantined records reference this item.")
            else:
                st.dataframe(qrows.drop(columns=["raw_json"], errors="ignore"), hide_index=True, width="stretch")
                for r in info["quarantine_rows"]:
                    with st.expander(f"raw payload of {r.get('natural_key')}"):
                        st.code(json.dumps(json.loads(r["raw_json"]), indent=2, ensure_ascii=False), language="json")
        with t5:
            b = info.get("bronze", {})
            if not b or not any(b.values()):
                st.write("No Bronze pointer (not a gateway item, or Bronze has been cleaned up).")
            else:
                st.dataframe(pd.DataFrame([x for v in b.values() for x in v]), hide_index=True, width="stretch")
