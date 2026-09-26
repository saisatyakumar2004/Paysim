"""The PaySim dashboard pages render against a real pipeline run, and the account-investigation filters work through the UI."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402
from test_paysim import synthetic  # noqa: E402

from payreconcile import config  # noqa: E402
from payreconcile.paysim import pipeline  # noqa: E402
from payreconcile.paysim.common import Dataset  # noqa: E402

DASH = str(Path(__file__).resolve().parents[1] / "dashboard")
PAGES = ["ps_overview", "ps_transactions", "ps_fraud", "ps_balance", "ps_account", "ps_dq", "ps_model"]


@pytest.fixture(scope="module")
def platform(tmp_path_factory):
    root = tmp_path_factory.mktemp("psdash")
    mp = pytest.MonkeyPatch()
    mp.setenv("PAYRECON_DATA", str(root))
    config.settings.cache_clear()
    ds = Dataset("full", synthetic(root / "src.csv", n=4000, seed=11))          # named "full" so the sample warning is not shown
    pipeline.run_pipeline(ds, with_profile=False, with_export=False)
    yield ds
    mp.undo()
    config.settings.cache_clear()


def _page(module: str, dash: str) -> None:
    import importlib
    import sys as _s
    if dash not in _s.path:
        _s.path.insert(0, dash)
    importlib.import_module(f"views.{module}").render()


@pytest.mark.parametrize("module", PAGES)
def test_every_paysim_page_renders_without_error(platform, module):
    at = AppTest.from_function(_page, args=(module, DASH), default_timeout=120).run()
    assert not at.exception, [str(e.value)[:300] for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    assert len(at.title) == 1


def test_the_overview_shows_the_required_kpis_from_the_data(platform):
    at = AppTest.from_function(_page, args=("ps_overview", DASH), default_timeout=120).run()
    labels = {m.label for m in at.metric}
    assert {"Total transactions", "Total transaction value", "Fraud transactions", "Fraud amount", "Fraud rate",
            "Flagged fraud (isFlaggedFraud)", "Data-quality failures (ERROR)"} <= labels
    total = next(m for m in at.metric if m.label == "Total transactions").value
    assert total == "4,000"


def test_the_account_page_filters_by_origin_type_and_fraud_status(platform):
    import duckdb
    con = duckdb.connect(str(platform.db), read_only=True)
    acct = con.execute("SELECT name_orig FROM gold.fact_transactions WHERE is_fraud = 1 LIMIT 1").fetchone()[0]
    n_all = con.execute("SELECT count(*) FROM gold.fact_transactions WHERE name_orig = ?", [acct]).fetchone()[0]
    con.close()
    at = AppTest.from_function(_page, args=("ps_account", DASH), default_timeout=120).run()
    at.text_input[0].set_value(acct).run()
    assert not at.exception, [str(e.value)[:300] for e in at.exception]
    shown = next(m for m in at.metric if m.label == "Transactions shown").value
    assert int(shown.split()[0].replace(",", "")) == n_all
    at.radio[0].set_value("not fraud").run()
    fraud_among = next(m for m in at.metric if m.label == "Fraud among them").value
    assert fraud_among == "0"


def test_the_entry_point_lists_the_paysim_pages_with_stable_urls():
    src = (Path(DASH) / "app.py").read_text(encoding="utf-8")
    for path in ("paysim-overview", "paysim-transactions", "paysim-fraud", "paysim-balance", "paysim-accounts", "paysim-data-quality",
                 "paysim-models"):
        assert f'"{path}"' in src
    assert '"money-flow", money_flow.render, True' in src or 'money_flow.render, True' in src           # the default page is unchanged
    assert sys.version_info >= (3, 11)
