"""Scan the repository for accidentally committed secrets or personal data.

Checks every git-tracked file (falls back to a directory walk) for: private keys, cloud/API tokens, hard-coded password or
key assignments, connection strings with credentials, e-mail addresses that are not obviously placeholders, and a few
personal identifiers. Also verifies that .env is ignored and that no .env file is tracked.

Usage:  python scripts/scan_secrets.py            (exit code 1 when anything is found)
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_SUFFIX = {".parquet", ".duckdb", ".png", ".jpg", ".pdf", ".ico", ".pyc"}

PATTERNS = {
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY-----"),
    "AWS access key id": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
    "Slack token": re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b"),
    "generic secret assignment": re.compile(r"(?i)\b(?:api[_-]?key|secret|token|passwd|password|pwd)\b\s*[:=]\s*['\"][^'\"\s]{8,}['\"]"),
    "credentials in a URL": re.compile(r"\b[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@"),
    "Bearer token": re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._-]{20,}"),
}
EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
EMAIL_OK = re.compile(r"(@example\.(com|org)|@users\.noreply|noreply@|@anthropic\.com|@localhost)", re.I)
PERSONAL = [re.compile(p, re.I) for p in (r"C:\\Users\\[A-Za-z0-9_.-]+", r"/home/[a-z][a-z0-9_-]+/", r"gmail\.com")]


def tracked_files() -> list[Path]:
    try:
        out = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.split("\n")
        return [ROOT / p for p in out if p]
    except (OSError, subprocess.CalledProcessError):
        return [p for p in ROOT.rglob("*") if p.is_file() and ".venv" not in p.parts and ".git" not in p.parts]


def main() -> int:
    findings: list[str] = []
    files = [f for f in tracked_files() if f.suffix.lower() not in SKIP_SUFFIX and f.is_file()]
    for f in files:
        rel = f.relative_to(ROOT).as_posix()
        if rel == "scripts/scan_secrets.py":
            continue
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for i, line in enumerate(text.splitlines(), start=1):
            for name, rx in PATTERNS.items():
                if rx.search(line):
                    findings.append(f"{rel}:{i}: {name}")
            for m in EMAIL.finditer(line):
                if not EMAIL_OK.search(m.group(0)):
                    findings.append(f"{rel}:{i}: e-mail address {m.group(0)!r}")
            for rx in PERSONAL:
                if rx.search(line):
                    findings.append(f"{rel}:{i}: personal/local identifier ({rx.pattern})")
        if f.name.startswith(".env") and f.name != ".env.example":
            findings.append(f"{rel}: an .env file is tracked")
    gi = (ROOT / ".gitignore").read_text(encoding="utf-8") if (ROOT / ".gitignore").exists() else ""
    if ".env" not in gi.split():
        findings.append(".gitignore does not ignore .env")
    if not (ROOT / ".env.example").exists():
        findings.append(".env.example is missing")
    print(f"scanned {len(files)} tracked files")
    for f in findings:
        print("FOUND:", f)
    print("OK: no secrets or personal data found" if not findings else f"{len(findings)} finding(s)")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
