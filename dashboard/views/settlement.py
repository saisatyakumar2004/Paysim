"""Settlement analytics: how long the bank takes, where unsettled money is ageing."""

from __future__ import annotations

import pandas as pd
import streamlit as st
from common import SYNTHETIC_NOTE, age_bucket, as_of, bucket_order, guard, inr, kpi_help, q

LAG = "(s.settlement_date - cast(t.created_at AS DATE))"
FROM = """FROM gold.recon_links l JOIN silver.gateway_txns t ON t.txn_id = l.txn_id
          JOIN silver.settlements s ON s.settlement_id = l.settlement_id
          LEFT JOIN silver.merchants_scd2 m ON m.merchant_id = t.merchant_id AND m.is_current
          WHERE t.txn_type = 'payment'"""


def render() -> None:
    guard()
    st.title("Settlement analytics")
    st.caption(f"Settlement lag = bank settlement date minus gateway capture date, for matched payments. {SYNTHETIC_NOTE}")
    st.caption("The source data has one bank feed and no settlement-partner dimension, so lag cannot be split by partner here.")

    o = q(f"SELECT avg({LAG}) AS mean, quantile_cont({LAG}, 0.5) AS p50, quantile_cont({LAG}, 0.95) AS p95, count(*) AS n {FROM}").iloc[0]
    c = st.columns(4)
    c[0].metric("Mean lag", f"{o['mean']:.2f} d", help=kpi_help("settlement_lag"))
    c[1].metric("Median lag", f"{o['p50']:.0f} d")
    c[2].metric("p95 lag", f"{o['p95']:.0f} d")
    c[3].metric("Settled payments", f"{int(o['n']):,}")

    left, right = st.columns(2)
    with left:
        st.subheader("Lag distribution (days)")
        dist = q(f"SELECT {LAG} AS lag_days, count(*) AS payments {FROM} GROUP BY 1 ORDER BY 1")
        st.bar_chart(dist.set_index("lag_days")["payments"])
    with right:
        st.subheader("Daily mean and p95 lag")
        daily = q(f"""SELECT cast(t.created_at AS DATE) AS txn_date, avg({LAG}) AS mean_lag, quantile_cont({LAG}, 0.95) AS p95_lag
                      {FROM} GROUP BY 1 ORDER BY 1""")
        st.line_chart(daily.set_index("txn_date"))

    a, b = st.columns(2)
    with a:
        st.subheader("Lag by payment method")
        st.dataframe(q(f"SELECT t.method, count(*) AS payments, round(avg({LAG}), 3) AS mean_lag, quantile_cont({LAG}, 0.95) AS p95_lag "
                       f"{FROM} GROUP BY 1 ORDER BY 3 DESC"), hide_index=True, width="stretch")
    with b:
        st.subheader("Lag by merchant category")
        st.dataframe(q(f"SELECT m.category, count(*) AS payments, round(avg({LAG}), 3) AS mean_lag, quantile_cont({LAG}, 0.95) AS p95_lag "
                       f"{FROM} GROUP BY 1 ORDER BY 3 DESC"), hide_index=True, width="stretch")

    st.subheader("Unsettled money ageing")
    asof = as_of()
    un = q("""SELECT status, event_date, expected_amount FROM gold.fact_reconciliation
              WHERE item_type = 'txn' AND status IN ('PENDING', 'MISSING_SETTLEMENT')""")
    if un.empty or asof is None:
        st.info("Nothing unsettled.")
    else:
        un["bucket"] = [age_bucket((pd.Timestamp(asof) - pd.Timestamp(d)).days) for d in un["event_date"]]
        piv = un.pivot_table(index="bucket", columns="status", values="expected_amount", aggfunc="sum").reindex(bucket_order()).fillna(0)
        st.bar_chart(piv)
        st.caption(f"PENDING = not yet due (inside the settlement SLA). MISSING_SETTLEMENT = past the SLA. Total unsettled: "
                   f"{inr(un['expected_amount'].sum())}.")
