from __future__ import annotations

import pandas as pd
import pytest

from payreconcile import config, db


@pytest.fixture()
def data_dir(tmp_path, monkeypatch):
    """Redirect every data path (source, bronze, warehouse, truth) to a temp directory."""
    monkeypatch.setenv("PAYRECON_DATA", str(tmp_path))
    config.settings.cache_clear()
    return tmp_path


@pytest.fixture()
def con(data_dir):
    connection = db.connect()
    connection.execute("""INSERT INTO silver.merchants_scd2 VALUES
        ('M1', 'Shop 1', 'grocery', 'Pune', 'low', 2.0, DATE '1970-01-01', DATE '9999-12-31', true,
         md5(concat_ws('|', 'Shop 1', 'grocery', 'Pune', 'low', 2.0::DOUBLE)), 'seed'),
        ('M2', 'Shop 2', 'fashion', 'Delhi', 'high', 1.5, DATE '1970-01-01', DATE '9999-12-31', true,
         md5(concat_ws('|', 'Shop 2', 'fashion', 'Delhi', 'high', 1.5::DOUBLE)), 'seed')""")
    yield connection
    connection.close()


GW_COLUMNS = ["txn_id", "order_id", "merchant_id", "amount", "currency", "status", "method", "txn_type",
              "refund_of", "created_at"]


def gw_row(**over) -> dict:
    row = {"txn_id": "TXN1", "order_id": "ORD1", "merchant_id": "M1", "amount": "100.0", "currency": "INR",
           "status": "captured", "method": "upi", "txn_type": "payment", "refund_of": None,
           "created_at": "2026-01-02T10:00:00"}
    row.update(over)
    return row


def gw_df(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=GW_COLUMNS).astype(object).where(lambda d: d.notna(), None)
