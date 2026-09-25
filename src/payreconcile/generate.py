"""Synthetic, seeded source-data generator with ground-truth labels.

Writes four kinds of source into <data>/source and the answer key into <data>/truth:

* gateway/<date>/page_NNN.json   paginated JSON (a file-based stand-in for a payment-gateway REST API)
* settlements/settlements_<date>.csv   bank settlement files (T+1 / T+2 lag)
* orders/orders.db                SQLite change log of the order system (CDC-style, updated_at)
* merchants/merchants_<date>.csv  weekly merchant-master snapshots (drives the SCD2 dimension)

Defects are injected at known rates and recorded in the truth tables, so the pipeline's
detection quality (precision/recall) can be *measured* instead of asserted.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from . import config

CATEGORIES = ["grocery", "electronics", "fashion", "travel", "food", "gaming", "pharmacy", "services"]
CITIES = ["Hyderabad", "Bengaluru", "Pune", "Chennai", "Delhi", "Mumbai", "Kolkata", "Ahmedabad"]
METHODS = ["upi", "card", "netbanking", "wallet"]
METHOD_P = [0.52, 0.30, 0.10, 0.08]

# (defect, base probability) applied to captured payment transactions in the gateway feed.
GATEWAY_DEFECTS = [
    ("dup_webhook", 0.015),
    ("null_order_id", 0.004),
    ("amount_symbol", 0.010),
    ("amount_negative", 0.003),
    ("amount_na", 0.002),
    ("bad_status", 0.005),
    ("bad_date", 0.004),
    ("future_date", 0.002),
    ("alt_date_format", 0.030),
    ("orphan_merchant", 0.003),
]
QUARANTINE_DEFECTS = {
    "null_order_id", "amount_negative", "amount_na", "bad_status", "bad_date", "future_date", "orphan_merchant",
}
# settlement outcome probabilities for observed captured transactions
SETTLE_OUTCOMES = [("missing", 0.015), ("mismatch", 0.015), ("rounding", 0.020), ("corrupt_ref", 0.010)]


@dataclass
class GenConfig:
    out: Path = field(default_factory=config.data_dir)
    days: int = 30
    start: date = date(2026, 1, 1)
    orders_per_day: int = 3000
    n_merchants: int = 200
    seed: int = 42
    bad_rate: float = 1.0          # multiplier on every defect probability (0 = clean data)
    page_size: int = 500


def _hour_weights() -> np.ndarray:
    w = np.array([1, 1, 1, 1, 1, 2, 3, 5, 6, 7, 7, 7, 7, 7, 7, 7, 8, 9, 10, 10, 9, 7, 4, 2], dtype=float)
    return w / w.sum()


def _pick(rng: np.random.Generator, table: list[tuple[str, float]], scale: float) -> str | None:
    r = rng.random()
    acc = 0.0
    for name, p in table:
        acc += p * scale
        if r < acc:
            return name
    return None


def make_merchants(rng: np.random.Generator, n: int) -> pd.DataFrame:
    return pd.DataFrame({
        "merchant_id": [f"M{i:04d}" for i in range(n)],
        "name": [f"Merchant {i}" for i in range(n)],
        "category": rng.choice(CATEGORIES, n),
        "city": rng.choice(CITIES, n),
        "risk_tier": rng.choice(["low", "medium", "high"], n, p=[0.6, 0.3, 0.1]),
        "fee_pct": np.round(rng.uniform(1.4, 2.6, n), 2),
    })


def evolve_merchants(rng: np.random.Generator, prev: pd.DataFrame) -> pd.DataFrame:
    cur = prev.copy()
    idx = rng.choice(len(cur), size=max(1, int(0.05 * len(cur))), replace=False)
    for i in idx:
        if rng.random() < 0.5:
            cur.loc[cur.index[i], "risk_tier"] = rng.choice(["low", "medium", "high"])
        else:
            cur.loc[cur.index[i], "fee_pct"] = round(float(rng.uniform(1.4, 2.6)), 2)
    return cur


def generate(cfg: GenConfig) -> dict:
    rng = np.random.default_rng(cfg.seed)
    out = Path(cfg.out)
    src, truth = out / "source", out / "truth"
    for p in (src, truth):
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True)

    bad = cfg.bad_rate
    last_day = cfg.days - 1
    hour_p = _hour_weights()
    sla_days = config.settings()["reconciliation"]["settlement_sla_days"]

    # ---- merchants ------------------------------------------------------------------------
    snapshots: dict[int, pd.DataFrame] = {0: make_merchants(rng, cfg.n_merchants)}
    for k in range(7, cfg.days, 7):
        snapshots[k] = evolve_merchants(rng, snapshots[max(snapshots)])
    weights = rng.lognormal(0, 1.0, cfg.n_merchants)
    weights /= weights.sum()
    merchant_ids = snapshots[0]["merchant_id"].to_numpy()

    def fee_pct_for(mid: str, d: int) -> float:
        snap = snapshots[max(k for k in snapshots if k <= d)]
        return float(snap.loc[snap["merchant_id"] == mid, "fee_pct"].iloc[0])

    dq_truth: list[dict] = []
    (src / "merchants").mkdir()
    for k, snap in snapshots.items():
        df = snap.copy()
        if k > 0 and bad > 0:   # defects only after the first snapshot so FK integrity starts clean
            i = int(rng.integers(len(df)))
            dup = df.iloc[[i]]
            df = pd.concat([df, dup], ignore_index=True)
            dq_truth.append({"dataset": "merchants", "key": dup["merchant_id"].iloc[0], "defect": "dup_row",
                             "action": "quarantine"})
            j = int(rng.integers(len(df) - 1))
            df.loc[j, "fee_pct"] = 25.0
            dq_truth.append({"dataset": "merchants", "key": df.loc[j, "merchant_id"], "defect": "bad_fee",
                             "action": "quarantine"})
        df.to_csv(src / "merchants" / f"merchants_{(cfg.start + timedelta(days=k)).isoformat()}.csv", index=False)

    # ---- anomaly plan (merchant-day level fraud patterns) -----------------------------------
    anomalies: dict[tuple[str, int], str] = {}
    n_anom = int(0.005 * cfg.n_merchants * cfg.days) if bad > 0 else 0
    for _ in range(n_anom):
        m = str(rng.choice(merchant_ids, p=weights))
        d = int(rng.integers(8, cfg.days)) if cfg.days > 8 else 0
        anomalies[(m, d)] = str(rng.choice(["velocity_burst", "card_testing", "refund_spike"]))

    # ---- transactions ---------------------------------------------------------------------
    used_ids: set[str] = set()

    def new_txn_id() -> str:
        while True:
            tid = f"TXN{int(rng.integers(10**9, 10**10)):010d}"
            if tid not in used_ids:
                used_ids.add(tid)
                return tid

    def ts_on(d: int) -> datetime:
        h = int(rng.choice(24, p=hour_p))
        return datetime.combine(cfg.start + timedelta(days=d), datetime.min.time()) + timedelta(
            hours=h, minutes=int(rng.integers(60)), seconds=int(rng.integers(60)))

    txns: list[dict] = []
    order_rows: list[dict] = []
    truth_orders: list[dict] = []
    pending_refunds: dict[int, list[dict]] = {}
    order_seq = 0

    def add_order(merchant: str, d: int, when: datetime | None = None, force_fail: bool = False,
                  amount: float | None = None) -> None:
        nonlocal order_seq
        order_seq += 1
        oid = f"ORD{order_seq:08d}"
        created = when or ts_on(d)
        amt = amount if amount is not None else round(float(np.clip(rng.lognormal(6.6, 0.9), 20, 80000)), 2)
        cust = f"C{int(rng.integers(1, 40000)):06d}"
        failed = force_fail or rng.random() < 0.06
        orphan_payment = (not failed) and rng.random() < 0.007 * bad
        no_payment = (not failed) and (not orphan_payment) and rng.random() < 0.005 * bad
        amt_diff = (not failed) and (not no_payment) and rng.random() < 0.005 * bad
        order_amount = round(amt + float(rng.uniform(5, 200)), 2) if amt_diff else amt
        status = "cancelled" if failed else "paid"

        order_row = {"order_id": oid, "merchant_id": merchant, "customer_id": cust, "order_amount": order_amount,
                     "order_status": status, "created_at": created, "updated_at": created}
        if not orphan_payment:
            row = dict(order_row)
            r = rng.random()
            if r < 0.003 * bad:
                row["order_amount"] = -abs(row["order_amount"])
                dq_truth.append({"dataset": "orders", "key": oid, "defect": "negative_amount", "action": "quarantine"})
            elif r < 0.006 * bad:
                row["order_status"] = "delivred"
                dq_truth.append({"dataset": "orders", "key": oid, "defect": "bad_status", "action": "quarantine"})
            order_rows.append(row)
        else:
            truth_orders.append({"order_id": oid, "issue": "NO_ORDER"})   # payment exists, order does not
        if no_payment:
            truth_orders.append({"order_id": oid, "issue": "ORDER_WITHOUT_PAYMENT"})
            return
        if amt_diff:
            truth_orders.append({"order_id": oid, "issue": "AMOUNT_DIFF"})

        method = str(rng.choice(METHODS, p=METHOD_P))
        base = dict(order_id=oid, merchant_id=merchant, amount=amt, currency="INR", method=method,
                    txn_type="payment", refund_of=None, day=created.date(), created_at=created,
                    defect=None, outcome=None, is_dup_charge=False, orphan_refund=False)
        if failed:
            txns.append({**base, "txn_id": new_txn_id(), "status": "failed"})
            return
        first = {**base, "txn_id": new_txn_id(), "status": "captured"}
        first["defect"] = _pick(rng, GATEWAY_DEFECTS, bad)
        txns.append(first)
        if rng.random() < 0.008 * bad:      # customer charged twice for the same order
            dup = {**base, "txn_id": new_txn_id(), "status": "captured", "is_dup_charge": True,
                   "created_at": created + timedelta(seconds=int(rng.integers(30, 300)))}
            txns.append(dup)
        if rng.random() < 0.03:             # refund later
            rd = (created.date() - cfg.start).days + int(rng.integers(1, 6))
            pending_refunds.setdefault(rd, []).append(first)

    for d in range(cfg.days):
        day = cfg.start + timedelta(days=d)
        n = max(50, int(cfg.orders_per_day * (0.85 if day.weekday() >= 5 else 1.05) * rng.normal(1, 0.05)))
        for m in rng.choice(merchant_ids, size=n, p=weights):
            add_order(str(m), d)
        for (m, ad), pattern in list(anomalies.items()):
            if ad != d:
                continue
            base_n = max(5, int(n * weights[list(merchant_ids).index(m)]))
            when0 = ts_on(d).replace(hour=int(rng.integers(0, 22)))
            if pattern == "velocity_burst":
                for _ in range(max(40, base_n * 8)):
                    add_order(m, d, when=when0 + timedelta(seconds=int(rng.integers(0, 7200))))
            elif pattern == "card_testing":
                for _ in range(80):
                    add_order(m, d, when=when0 + timedelta(seconds=int(rng.integers(0, 3600))), force_fail=True,
                              amount=round(float(rng.uniform(1, 10)), 2))
        # refunds that come due today
        for orig in pending_refunds.pop(d, []):
            if orig["defect"] in QUARANTINE_DEFECTS:
                continue
            partial = rng.random() < 0.3
            amt = round(orig["amount"] * float(rng.uniform(0.3, 0.9)), 2) if partial else orig["amount"]
            refund_of = orig["txn_id"]
            orphan = rng.random() < 0.03 * bad     # 3% of refunds reference a txn that never existed
            if orphan:
                refund_of = new_txn_id()
            txns.append({**orig, "txn_id": new_txn_id(), "txn_type": "refund", "amount": amt, "refund_of": refund_of,
                         "created_at": ts_on(d), "day": day, "defect": None, "outcome": None, "is_dup_charge": False,
                         "orphan_refund": orphan, "status": "captured"})
        # refund_spike: mass refunds of the merchant's recent payments created today
        for (m, ad), pattern in anomalies.items():
            if ad == d and pattern == "refund_spike":
                recent = [t for t in txns if t["merchant_id"] == m and t["txn_type"] == "payment"
                          and t["status"] == "captured" and t["defect"] is None and t["day"] < day
                          and (day - t["day"]).days <= 5]
                for orig in recent[: max(15, int(0.4 * len(recent)))]:
                    txns.append({**orig, "txn_id": new_txn_id(), "txn_type": "refund", "refund_of": orig["txn_id"],
                                 "created_at": ts_on(d), "day": day, "defect": None, "outcome": None,
                                 "is_dup_charge": False, "orphan_refund": False, "status": "captured"})
    # refund status changes on the order (change-log rows)
    orders_by_id = {r["order_id"]: r for r in order_rows}
    for t in txns:
        if t["txn_type"] == "refund" and (t["created_at"].date() - cfg.start).days <= last_day:
            base = orders_by_id.get(t["order_id"])
            if base is not None and not t["orphan_refund"]:
                order_rows.append({**base, "order_status": "refunded", "updated_at": t["created_at"]})

    # ---- observed window: drop anything created after the last day -------------------------
    txns = [t for t in txns if (t["created_at"].date() - cfg.start).days <= last_day]

    # ---- settlements ------------------------------------------------------------------------
    settle_rows: list[dict] = []
    truth_settle: list[dict] = []
    stl_seq = 0
    for t in txns:
        t["settle_day"] = None
        if t["status"] != "captured":
            continue
        d = (t["day"] - cfg.start).days
        quarantined_defect = t["defect"] in QUARANTINE_DEFECTS
        outcome = "ok" if quarantined_defect else (_pick(rng, SETTLE_OUTCOMES, bad) or "ok")
        t["outcome"] = outcome
        lag = 1 if rng.random() < 0.7 else 2
        sd = d + lag
        t["settle_day"] = sd
        if outcome == "missing" or sd > last_day:
            continue
        sign = -1.0 if t["txn_type"] == "refund" else 1.0
        gross = round(sign * t["amount"], 2)
        if outcome == "mismatch":
            gross = round(gross * (1 - float(rng.uniform(0.02, 0.30))), 2)
            if abs(abs(gross) - t["amount"]) <= 1.0:
                gross = round(sign * (t["amount"] - 5.0), 2)
        elif outcome == "rounding":
            gross = round(gross + float(rng.uniform(0.01, 0.49)) * (1 if rng.random() < 0.5 else -1), 2)
        fee = 0.0 if t["txn_type"] == "refund" else round(abs(gross) * fee_pct_for(t["merchant_id"], d) / 100, 2)
        net = round(gross - fee, 2)
        net_defect = rng.random() < 0.003 * bad
        if net_defect:
            net = round(net + 5.0, 2)
        ref_id = t["txn_id"]
        if outcome == "corrupt_ref":
            pos = int(rng.integers(3, len(ref_id)))
            ref_id = ref_id[:pos] + str((int(ref_id[pos]) + int(rng.integers(1, 9))) % 10) + ref_id[pos + 1:]
        ref = f"GW-{ref_id}" if rng.random() < 0.6 else ref_id.lower()
        stl_seq += 1
        sid = f"STL{stl_seq:09d}"
        settle_rows.append({"settlement_id": sid, "gateway_ref": ref, "merchant_id": t["merchant_id"],
                            "gross_amount": gross, "fee_amount": fee, "net_amount": net,
                            "settlement_date": (cfg.start + timedelta(days=sd)).isoformat(), "_day": sd})
        truth_settle.append({"settlement_id": sid, "txn_id": t["txn_id"], "kind": outcome,
                             "expected_status": "SOURCE_QUARANTINED" if quarantined_defect else "LINKED"})
        if net_defect:
            dq_truth.append({"dataset": "settlements", "key": sid, "defect": "net_mismatch", "action": "flag"})

    n_orphans = int(0.004 * len(settle_rows) * bad)
    for _ in range(n_orphans):
        sd = int(rng.integers(1, cfg.days))
        stl_seq += 1
        sid = f"STL{stl_seq:09d}"
        amt = round(float(np.clip(rng.lognormal(6.6, 0.9), 20, 80000)), 2)
        m = str(rng.choice(merchant_ids, p=weights))
        settle_rows.append({"settlement_id": sid, "gateway_ref": f"GW-TXN{int(rng.integers(10**9, 10**10)):010d}",
                            "merchant_id": m, "gross_amount": amt, "fee_amount": round(amt * 0.02, 2),
                            "net_amount": round(amt * 0.98, 2),
                            "settlement_date": (cfg.start + timedelta(days=sd)).isoformat(), "_day": sd})
        truth_settle.append({"settlement_id": sid, "txn_id": None, "kind": "orphan", "expected_status": "ORPHAN_SETTLEMENT"})

    # ---- write settlements ------------------------------------------------------------------
    (src / "settlements").mkdir()
    sdf = pd.DataFrame(settle_rows)
    cols = ["settlement_id", "gateway_ref", "merchant_id", "gross_amount", "fee_amount", "net_amount", "settlement_date"]
    for d in range(cfg.days):
        part = sdf[sdf["_day"] == d][cols].copy() if len(sdf) else pd.DataFrame(columns=cols)
        if part.empty:
            continue
        if bad > 0 and rng.random() < 0.005 * len(part) / 10:   # bank re-sends a row
            dupe = part.sample(1, random_state=int(rng.integers(1_000_000)))
            part = pd.concat([part, dupe], ignore_index=True)
            dq_truth.append({"dataset": "settlements", "key": dupe["settlement_id"].iloc[0], "defect": "dup_row",
                             "action": "quarantine"})
        if d == 12 and bad > 0:
            part = part.rename(columns={"gateway_ref": "txn_reference"})       # known column rename (alias)
        if d == 20 and bad > 0:
            part["batch_note"] = "manual adjustment"                           # unexpected extra column
        part.to_csv(src / "settlements" / f"settlements_{(cfg.start + timedelta(days=d)).isoformat()}.csv", index=False)

    # ---- write gateway pages ------------------------------------------------------------------
    def gateway_record(t: dict) -> dict:
        created = t["created_at"]
        rec = {"txn_id": t["txn_id"], "order_id": t["order_id"], "merchant_id": t["merchant_id"],
               "amount": t["amount"], "currency": "INR", "status": t["status"], "method": t["method"],
               "txn_type": t["txn_type"], "refund_of": t["refund_of"], "created_at": created.strftime("%Y-%m-%dT%H:%M:%S")}
        df = t["defect"]
        if df == "null_order_id":
            rec["order_id"] = None
        elif df == "amount_symbol":
            rec["amount"] = f"₹{t['amount']:,.2f}"
        elif df == "amount_negative":
            rec["amount"] = -t["amount"]
        elif df == "amount_na":
            rec["amount"] = "N/A"
        elif df == "bad_status":
            rec["status"] = "captred"
        elif df == "bad_date":
            rec["created_at"] = f"{created.year}-02-30T{created.strftime('%H:%M:%S')}"
        elif df == "future_date":
            rec["created_at"] = f"{created.year + 4}{created.strftime('-%m-%dT%H:%M:%S')}"
        elif df == "alt_date_format":
            rec["created_at"] = created.strftime("%d/%m/%Y %H:%M:%S")
        elif df == "orphan_merchant":
            rec["merchant_id"] = "M9999"
        return rec

    by_arrival: dict[int, list[dict]] = {d: [] for d in range(cfg.days)}
    for t in txns:
        d = (t["day"] - cfg.start).days
        arrival = d
        if rng.random() < 0.01 * bad and d + 2 <= last_day:      # late-arriving record
            arrival = d + int(rng.integers(1, 3))
        rec = gateway_record(t)
        by_arrival[arrival].append(rec)
        if t["defect"]:
            action = "quarantine" if t["defect"] in QUARANTINE_DEFECTS or t["defect"] == "dup_webhook" else "repair"
            dq_truth.append({"dataset": "gateway", "key": t["txn_id"], "defect": t["defect"], "action": action})
        if t["defect"] == "dup_webhook":
            by_arrival[arrival].append(dict(rec))
    (src / "gateway").mkdir()
    for d, recs in by_arrival.items():
        if not recs:
            continue
        if d == 15 and bad > 0:
            for r in recs:
                r["risk_score"] = round(float(rng.random()), 3)         # unexpected new column
        folder = src / "gateway" / (cfg.start + timedelta(days=d)).isoformat()
        folder.mkdir()
        pages = [recs[i:i + cfg.page_size] for i in range(0, len(recs), cfg.page_size)]
        for i, page in enumerate(pages, start=1):
            payload = {"page": i, "next_page": i + 1 if i < len(pages) else None, "data": page}
            (folder / f"page_{i:03d}.json").write_text(json.dumps(payload), encoding="utf-8")

    # ---- orders (SQLite change log) ---------------------------------------------------------
    (src / "orders").mkdir()
    con = sqlite3.connect(src / "orders" / "orders.db")
    con.execute("""CREATE TABLE order_changes (order_id TEXT, merchant_id TEXT, customer_id TEXT, order_amount REAL,
                   order_status TEXT, created_at TEXT, updated_at TEXT)""")
    con.execute("CREATE INDEX ix_updated ON order_changes(updated_at)")
    con.executemany(
        "INSERT INTO order_changes VALUES (?,?,?,?,?,?,?)",
        [(r["order_id"], r["merchant_id"], r["customer_id"], r["order_amount"], r["order_status"],
          r["created_at"].strftime("%Y-%m-%d %H:%M:%S"), r["updated_at"].strftime("%Y-%m-%d %H:%M:%S"))
         for r in order_rows if (r["updated_at"].date() - cfg.start).days <= last_day])
    con.commit()
    con.close()

    # ---- truth tables -------------------------------------------------------------------------
    as_of = last_day
    settled_ok = {s["txn_id"]: s for s in truth_settle if s["txn_id"]}
    truth_rows = []
    quarantined_keys = {r["key"] for r in dq_truth if r["dataset"] == "gateway" and r["action"] == "quarantine"
                        and r["defect"] != "dup_webhook"}
    dup_charge_ids = {t["txn_id"] for t in txns if t["is_dup_charge"]}
    for t in txns:
        if t["status"] != "captured":
            continue
        d = (t["day"] - cfg.start).days
        if t["txn_id"] in quarantined_keys:
            status = "QUARANTINED"
        elif t["orphan_refund"]:
            status = "ORPHAN_REFUND"
        elif t["txn_id"] in settled_ok:
            kind = settled_ok[t["txn_id"]]["kind"]
            status = "AMOUNT_MISMATCH" if kind == "mismatch" else "MATCHED"
        else:
            status = "MISSING_SETTLEMENT" if d + sla_days < as_of else "PENDING"
        truth_rows.append({"txn_id": t["txn_id"], "txn_type": t["txn_type"], "merchant_id": t["merchant_id"],
                           "txn_date": t["day"], "amount": t["amount"], "defect": t["defect"], "outcome": t["outcome"],
                           "expected_status": status, "is_duplicate_charge": t["txn_id"] in dup_charge_ids,
                           "expected_fuzzy": bool(t["outcome"] == "corrupt_ref" and status == "MATCHED")})
    pd.DataFrame(truth_rows).to_parquet(truth / "txn.parquet", index=False)
    pd.DataFrame(truth_settle).to_parquet(truth / "settlement.parquet", index=False)
    pd.DataFrame(truth_orders or [{"order_id": "", "issue": ""}]).to_parquet(truth / "orders.parquet", index=False)
    pd.DataFrame(dq_truth or [{"dataset": "", "key": "", "defect": "", "action": ""}]).to_parquet(
        truth / "dq.parquet", index=False)
    pd.DataFrame([{"merchant_id": m, "date": cfg.start + timedelta(days=d), "pattern": p}
                  for (m, d), p in anomalies.items()] or [{"merchant_id": "", "date": cfg.start, "pattern": ""}]
                 ).to_parquet(truth / "anomaly.parquet", index=False)

    summary = {
        "days": cfg.days, "orders": order_seq, "gateway_records": int(sum(len(v) for v in by_arrival.values())),
        "settlements": len(settle_rows), "seed": cfg.seed, "bad_rate": bad,
        "injected": pd.DataFrame(dq_truth)["defect"].value_counts().to_dict() if dq_truth else {},
        "anomalies": len(anomalies),
    }
    (truth / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return summary
