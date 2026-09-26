"""PayReconcile dashboards.

Run:  streamlit run dashboard/app.py
Two groups. PayReconcile: nine pages (/ = money-flow, /overview, /reconciliation, /exceptions, /data-quality, /settlement,
/merchant-risk, /anomalies, /pipeline). PaySim analytics: seven pages (/paysim-overview, /paysim-transactions, /paysim-fraud,
/paysim-balance, /paysim-accounts, /paysim-data-quality, /paysim-models). Reads the databases read-only; nothing here can modify data.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from views import (  # noqa: E402
    anomaly_signals,
    data_quality,
    exceptions,
    executive,
    merchant_risk,
    money_flow,
    pipeline_health,
    ps_account,
    ps_balance,
    ps_dq,
    ps_fraud,
    ps_model,
    ps_overview,
    ps_transactions,
    reconciliation,
    settlement,
)

st.set_page_config(page_title="PayReconcile", layout="wide")

PAGES = [
    ("Money Flow", "money-flow", money_flow.render, True),
    ("Executive Overview", "overview", executive.render, False),
    ("Reconciliation", "reconciliation", reconciliation.render, False),
    ("Exceptions Queue", "exceptions", exceptions.render, False),
    ("Data Quality", "data-quality", data_quality.render, False),
    ("Settlement Analytics", "settlement", settlement.render, False),
    ("Merchant Risk", "merchant-risk", merchant_risk.render, False),
    ("Anomaly Signals", "anomalies", anomaly_signals.render, False),
    ("Pipeline Health", "pipeline", pipeline_health.render, False),
]

PAYSIM_PAGES = [
    ("PaySim Overview", "paysim-overview", ps_overview.render),
    ("Transaction Analysis", "paysim-transactions", ps_transactions.render),
    ("Fraud Analysis", "paysim-fraud", ps_fraud.render),
    ("Balance Integrity", "paysim-balance", ps_balance.render),
    ("Account Investigation", "paysim-accounts", ps_account.render),
    ("PaySim Data Quality", "paysim-data-quality", ps_dq.render),
    ("Model Evaluation", "paysim-models", ps_model.render),
]

navigation = st.navigation({
    "PayReconcile": [st.Page(fn, title=title, url_path=path, default=default) for title, path, fn, default in PAGES],
    "PaySim analytics": [st.Page(fn, title=title, url_path=path) for title, path, fn in PAYSIM_PAGES],
})
navigation.run()
