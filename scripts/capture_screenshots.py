"""Capture real screenshots of the running dashboard and the rendered diagrams with a headless Edge/Chrome.

Starts Streamlit on a free port, loads each page URL in a headless browser, saves PNGs to docs/images/. No browser download
is needed (uses the Edge or Chrome that is installed). Requires a built warehouse (run the pipeline first).

Usage:  python scripts/capture_screenshots.py [--width 1500] [--height 1500]
"""

from __future__ import annotations

import argparse
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
OUT = ROOT / "docs" / "images"

BROWSERS = [r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"]


def find_browser() -> str:
    for b in BROWSERS:
        if Path(b).exists():
            return b
    raise SystemExit("No Edge or Chrome found")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def shot(browser: str, url: str, out: Path, w: int, h: int, budget_ms: int = 30000) -> bool:
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()
    cmd = [browser, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--window-size={w},{h}",
           f"--virtual-time-budget={budget_ms}", f"--screenshot={out}", url]
    subprocess.run(cmd, capture_output=True, timeout=180)
    return out.exists() and out.stat().st_size > 5000


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--width", type=int, default=1500)
    ap.add_argument("--height", type=int, default=1500)
    args = ap.parse_args()
    from payreconcile import config

    browser = find_browser()
    con = duckdb.connect(str(config.warehouse_path()), read_only=True)
    fuzzy = con.execute("SELECT txn_id FROM gold.recon_match_evidence WHERE method = 'fuzzy' AND decision = 'LINKED' LIMIT 1").fetchone()
    con.close()

    port = free_port()
    proc = subprocess.Popen([sys.executable, "-m", "streamlit", "run", str(ROOT / "dashboard" / "app.py"), "--server.headless", "true",
                             "--server.port", str(port), "--browser.gatherUsageStats", "false"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    results = {}
    try:
        for _ in range(60):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/_stcore/health", timeout=1)
                break
            except OSError:
                time.sleep(1)
        base = f"http://127.0.0.1:{port}"
        pages = {"dashboard_reconciliation.png": "/reconciliation",
                 "dashboard_exceptions_queue.png": "/exceptions",
                 "dashboard_data_quality.png": "/data-quality", "dashboard_settlement_analytics.png": "/settlement",
                 "dashboard_merchant_risk.png": "/merchant-risk", "dashboard_anomaly_signals.png": "/anomalies",
                 "dashboard_pipeline_health.png": "/pipeline"}
        pages["dashboard_executive_overview.png"] = "/"        # last: the first page load after start-up is cold
        if fuzzy:
            pages["dashboard_exception_explain.png"] = f"/exceptions?item=TXN:{fuzzy[0]}"
        warm = OUT / "_warmup.png"
        for name, path in pages.items():
            # warm Streamlit's query cache (30 s TTL) and capture right away, so the real capture is fast
            shot(browser, base + path, warm, args.width, args.height, 60000)
            results[name] = shot(browser, base + path, OUT / name, args.width, args.height, 60000)
            print(("ok   " if results[name] else "FAIL ") + name, flush=True)
        warm.unlink(missing_ok=True)
        import re
        for svg, png in (("architecture.svg", "architecture.png"), ("dbt_lineage.svg", "dbt_lineage.png")):
            src = OUT / svg
            if src.exists():
                size = re.search(r"width='(\d+)' height='(\d+)'", src.read_text(encoding="utf-8"))
                w, h = (int(size.group(1)), int(size.group(2))) if size else (1500, 800)
                results[png] = shot(browser, src.as_uri(), OUT / png, w, h, 5000)
                print(("ok   " if results[png] else "FAIL ") + png, flush=True)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
