"""PayReconcile dashboards.

Run:  streamlit run dashboard/app.py
Eight pages, each with its own URL (/overview, /reconciliation, /exceptions, /data-quality, /settlement, /merchant-risk,
/anomalies, /pipeline). Reads the warehouse read-only; nothing here can modify data.
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
    pipeline_health,
    reconciliation,
    settlement,
)

st.set_page_config(page_title="PayReconcile", layout="wide")

PAGES = [
    ("Executive Overview", "overview", executive.render, True),
    ("Reconciliation", "reconciliation", reconciliation.render, False),
    ("Exceptions Queue", "exceptions", exceptions.render, False),
    ("Data Quality", "data-quality", data_quality.render, False),
    ("Settlement Analytics", "settlement", settlement.render, False),
    ("Merchant Risk", "merchant-risk", merchant_risk.render, False),
    ("Anomaly Signals", "anomalies", anomaly_signals.render, False),
    ("Pipeline Health", "pipeline", pipeline_health.render, False),
]

navigation = st.navigation([st.Page(fn, title=title, url_path=path, default=default) for title, path, fn, default in PAGES])
navigation.run()
