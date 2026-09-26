"""Shared helpers for the PaySim dashboard pages: dataset selection, cached read-only queries, formatting."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT / "src", ROOT / "dashboard"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from payreconcile import config  # noqa: E402
from payreconcile.paysim import queries  # noqa: E402,F401  (re-exported for the pages)
from payreconcile.paysim.common import connect, dataset  # noqa: E402

SAMPLE_NOTE = ("This is a development SAMPLE: fraud is deliberately over-represented (about 6x), so rates shown here are not "
               "population rates. Select the full dataset for population figures.")


def available() -> list[str]:
    base = config.data_dir() / "paysim"
    if not base.exists():
        return []
    names = [p.name for p in base.iterdir() if (p / "paysim.duckdb").exists()]
    return sorted(names, key=lambda n: (n != "full", n))


def current() -> str:
    names = available()
    if not names:
        st.error("No PaySim data found. Run `python -m payreconcile paysim sample`, then `python -m payreconcile paysim run --dataset dev` "
                 "(and `--dataset full` for the whole file).")
        st.stop()
    label = st.sidebar.selectbox("PaySim dataset", names, key="paysim_dataset",
                                 help="full = the whole source file; dev = the reproducible 250k sample; sample_N = other sample sizes")
    return label


@st.cache_data(ttl=60, show_spinner=False)
def pq(name: str, sql: str, params: tuple = ()) -> pd.DataFrame:
    con = connect(dataset(name), read_only=True)
    try:
        return con.execute(sql, list(params)).df()
    finally:
        con.close()


def note_if_sample(name: str) -> None:
    if name != "full":
        st.warning(SAMPLE_NOTE)


def inr(x) -> str:
    return "n/a" if x is None or pd.isna(x) else f"{x:,.0f}"


def pct(x, digits: int = 3) -> str:
    return "n/a" if x is None or pd.isna(x) else f"{x:.{digits}%}"
