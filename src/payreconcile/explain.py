"""Explain a reconciliation decision: what the item is, why records were (not) matched, how its status changed,
and where the underlying records came from (Silver row, Bronze file, quarantine rows)."""

from __future__ import annotations

from typing import Any

from . import config


def _rows(con, sql: str, params: list | None = None) -> list[dict[str, Any]]:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def _bronze_pointer(con, txn_id: str) -> list[dict[str, Any]]:
    """Locate the raw Bronze record(s) of a gateway txn (file, run, ingest time). Empty if Bronze is unavailable."""
    pattern = (config.bronze_dir() / "gateway" / "*" / "part.parquet").as_posix()
    try:
        return _rows(con, f"SELECT _source_file, _run_id, _ingested_at, filename AS bronze_file "
                          f"FROM read_parquet('{pattern}', filename = true) WHERE txn_id = ?", [txn_id])
    except Exception:                                   # noqa: BLE001 - Bronze may have been cleaned up; not fatal
        return []


def explain(con, item_id: str, include_bronze: bool = True) -> dict[str, Any]:
    """Everything needed to answer "why is this item in this status, and can I trace it to the source?"."""
    item = _rows(con, "SELECT * FROM gold.fact_reconciliation WHERE item_id = ?", [item_id])
    out: dict[str, Any] = {"item_id": item_id, "current": item[0] if item else None,
                           "audit_history": _rows(con, "SELECT audit_id, run_date, as_of_date, change_kind, reason, "
                                                       "old_status, new_status, old_settlement_id, new_settlement_id, "
                                                       "old_money_at_risk, new_money_at_risk FROM gold.reconciliation_audit "
                                                       "WHERE item_id = ? ORDER BY audit_id", [item_id])}
    kind, _, key = item_id.partition(":")
    txn_ids: list[str] = []
    stl_ids: list[str] = []
    order_ids: list[str] = []
    if kind == "TXN":
        txn_ids = [key]
    elif kind == "STL":
        stl_ids = [key]
    elif kind == "ORD":
        order_ids = [key]
    cur = item[0] if item else {}
    if cur.get("txn_id"):
        txn_ids = [cur["txn_id"]]
    if cur.get("settlement_id"):
        stl_ids = sorted(set(stl_ids) | {cur["settlement_id"]})
    if cur.get("order_id"):
        order_ids = sorted(set(order_ids) | {cur["order_id"]})

    src: dict[str, Any] = {}
    if txn_ids:
        src["gateway_txns"] = _rows(con, f"SELECT * FROM silver.gateway_txns WHERE txn_id IN ({','.join('?' * len(txn_ids))})", txn_ids)
    if stl_ids:
        src["settlements"] = _rows(con, f"SELECT * FROM silver.settlements WHERE settlement_id IN ({','.join('?' * len(stl_ids))})", stl_ids)
    if order_ids:
        src["orders"] = _rows(con, f"SELECT * FROM silver.orders WHERE order_id IN ({','.join('?' * len(order_ids))})", order_ids)
        src["gateway_txns_for_order"] = _rows(con, f"SELECT txn_id, status, txn_type, amount, created_at FROM silver.gateway_txns "
                                                   f"WHERE order_id IN ({','.join('?' * len(order_ids))})", order_ids)
    out["silver_records"] = src

    ev: list[dict[str, Any]] = []
    if txn_ids:
        ev += _rows(con, f"SELECT * FROM gold.recon_match_evidence WHERE txn_id IN ({','.join('?' * len(txn_ids))})", txn_ids)
    if stl_ids:
        ev += _rows(con, f"SELECT * FROM gold.recon_match_evidence WHERE settlement_id IN ({','.join('?' * len(stl_ids))})", stl_ids)
    seen, unique = set(), []
    for e in ev:
        k = (e["txn_id"], e["settlement_id"], e["method"])
        if k not in seen:
            seen.add(k)
            unique.append(e)
    out["match_evidence"] = unique

    q: list[dict[str, Any]] = []
    keys = txn_ids + [c for c in ([cur.get("duplicate_of")] if cur.get("duplicate_of") else [])]
    if keys:
        q += _rows(con, f"SELECT dataset, natural_key, order_id, rule_ids, run_date, raw_json FROM quarantine.rows "
                        f"WHERE natural_key IN ({','.join('?' * len(keys))})", keys)
    if order_ids:
        q += _rows(con, f"SELECT dataset, natural_key, order_id, rule_ids, run_date, raw_json FROM quarantine.rows "
                        f"WHERE order_id IN ({','.join('?' * len(order_ids))}) OR "
                        f"(dataset = 'orders' AND natural_key IN ({','.join('?' * len(order_ids))}))", order_ids + order_ids)
    out["quarantine_rows"] = [dict(t) for t in {tuple(sorted(r.items())) for r in q}]
    if include_bronze and txn_ids:
        out["bronze"] = {t: _bronze_pointer(con, t) for t in txn_ids}
    return out
