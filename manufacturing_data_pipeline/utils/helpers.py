"""
Utility helpers for the manufacturing data pipeline.

This module keeps small, reusable functions that are shared across
data generation, cleaning, transformations, and analysis.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict

import pandas as pd


# -- Path constants (everything is relative to where this project lives) --

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
CHARTS_DIR = OUTPUT_DIR / "charts"
REPORTS_DIR = OUTPUT_DIR / "reports"


def ensure_directories() -> None:
    """Create data and output directories if they do not exist."""

    for path in (DATA_DIR, OUTPUT_DIR, CHARTS_DIR, REPORTS_DIR):
        path.mkdir(parents=True, exist_ok=True)


def get_data_path(filename: str) -> Path:
    """Return full path for a file inside the data directory."""
    return DATA_DIR / filename


def get_output_chart_path(filename: str) -> Path:
    """Return full path for a chart image inside the charts directory."""
    return CHARTS_DIR / filename


def get_report_path(filename: str) -> Path:
    """Return full path for a report file inside the reports directory."""
    return REPORTS_DIR / filename


def write_csv(df: pd.DataFrame, path: Path, index: bool = False) -> None:
    """Write a DataFrame to CSV with common settings.

    Using a single helper keeps I/O behavior consistent.
    Retries up to 3 times on PermissionError (common with OneDrive file locks).
    """

    path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(3):
        try:
            if path.exists():
                os.remove(path)
            df.to_csv(path, index=index)
            return
        except PermissionError:
            if attempt < 2:
                time.sleep(1)
            else:
                raise


def read_csv(path: Path) -> pd.DataFrame:
    """Read a CSV file into a DataFrame with common settings."""
    return pd.read_csv(path)


def default_random_seed() -> int:
    """Return the default random seed for reproducible runs."""
    return 42


def update_dict(base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy of `base` with `updates` applied.

    This is a small helper to keep configuration merging explicit and clear.
    """

    new_config = dict(base)
    new_config.update(updates)
    return new_config
