"""
KPI metrics - headline numbers that summarize plant performance.

Each function returns a single number (float) rather than a table.
  - OEE  : Overall Equipment Effectiveness
  - FPY  : First Pass Yield (% of units that passed without rework)
  - DPU  : Defects Per Unit
  - COPQ : Cost of Poor Quality (dollar estimate)
  - Andon response time
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def calc_fpy(df: pd.DataFrame) -> float:
    """First Pass Yield - what fraction of units passed without any rework.

    FPY = good units (no defect AND no rework) / total units
    """

    total = len(df)
    if total == 0:
        return float("nan")

    good = (df["Defect_Normalized"] == "None") & (df["Rework_Flag"] == 0)
    return float(good.sum() / total)


def calc_oee(df: pd.DataFrame) -> float:
    """Overall Equipment Effectiveness - a rough estimate.

    OEE = Availability x Performance x Quality
    - Availability : how often machines were actually running
    - Performance  : how fast vs. a 60-second target cycle time
    - Quality      : First Pass Yield
    """

    total = len(df)
    if total == 0:
        return float("nan")

    # Machines in downtime (Andon pulled or under maintenance)
    downtime = ((df["Andon_Flag"] == 1) | (df["Maintenance_Flag"] == 1)).sum()
    availability = 1.0 - downtime / total

    # Speed compared to a nominal 60-second cycle
    ct = pd.to_numeric(df["Cycle_Time"], errors="coerce")
    nominal = 60.0
    performance = (nominal / ct.clip(lower=1)).clip(upper=1.2).mean()

    quality = calc_fpy(df)

    oee = availability * performance * quality
    return float(oee)


def calc_dpu(df: pd.DataFrame) -> float:
    """Defects Per Unit - what fraction of events had a defect."""

    total = len(df)
    if total == 0:
        return float("nan")

    defective = df["Defect_Normalized"].isin(["Reject", "Repair"])
    return float(defective.sum() / total)


def calc_copq(df: pd.DataFrame) -> float:
    """Cost of Poor Quality - simple dollar estimate.

    Uses fixed costs: $100 per rejected unit, $30 per repaired unit.
    """

    base_scrap_cost = 100.0
    base_rework_cost = 30.0

    scrap_events = df["Defect_Normalized"] == "Reject"
    repair_events = df["Defect_Normalized"] == "Repair"

    copq = scrap_events.sum() * base_scrap_cost + repair_events.sum() * base_rework_cost
    return float(copq)


def andon_response_time_metric(df: pd.DataFrame) -> float:
    """Estimate how quickly the line responds after an Andon pull.

    Measures the average time gap (in seconds) between an Andon event
    and the very next event on the line.
    """

    if "TS_dt" not in df.columns:
        return float("nan")

    df_sorted = df.sort_values("TS_dt")
    times = df_sorted["TS_dt"]
    andon_idx = df_sorted.index[df_sorted["Andon_Flag"] == 1]
    if len(andon_idx) == 0:
        return float("nan")

    deltas = []
    for idx in andon_idx:
        pos = df_sorted.index.get_loc(idx)
        if pos + 1 < len(df_sorted):
            delta = (times.iloc[pos + 1] - times.iloc[pos]).total_seconds()
            if delta >= 0:
                deltas.append(delta)
    if not deltas:
        return float("nan")
    return float(np.mean(deltas))
