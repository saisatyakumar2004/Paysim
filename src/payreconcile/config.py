"""Paths and settings. The data directory can be redirected with PAYRECON_DATA (tests use this)."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = ROOT / "contracts"
DBT_DIR = ROOT / "dbt"


def data_dir() -> Path:
    return Path(os.environ.get("PAYRECON_DATA", ROOT / "data"))


def source_dir() -> Path:
    return data_dir() / "source"


def bronze_dir() -> Path:
    return data_dir() / "bronze"


def truth_dir() -> Path:
    return data_dir() / "truth"


def warehouse_path() -> Path:
    return data_dir() / "warehouse.duckdb"


@lru_cache(maxsize=1)
def settings() -> dict:
    with open(ROOT / "config" / "settings.yml", encoding="utf-8") as fh:
        return yaml.safe_load(fh)
