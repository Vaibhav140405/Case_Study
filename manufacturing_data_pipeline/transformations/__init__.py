"""
Transformations package - compute KPIs and summary tables.

Split into two files:
  - kpi_metrics.py  : single-number performance indicators (OEE, FPY, etc.)
  - aggregations.py : grouped summary tables (defect pareto, bottlenecks, etc.)

This file provides run_all_metrics() which runs everything at once.
"""

from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd

from utils import get_report_path, write_csv

from transformations.kpi_metrics import (
    calc_oee,
    calc_fpy,
    calc_dpu,
    calc_copq,
    andon_response_time_metric,
)
from transformations.aggregations import (
    defect_pareto,
    cycle_time_distribution,
    identify_bottlenecks,
    build_traceability_chain,
    throughput_vs_wip,
    pfmea_risk_score,
    energy_per_vehicle,
    detect_rework_loops,
    supplier_ppm,
    tool_calibration_kpi,
    shift_performance,
    warranty_early_warning,
    line_balance_metric,
    takt_time_adherence,
    plan_vs_actual,
)


def run_all_metrics(df: pd.DataFrame) -> Tuple[Dict[str, float], Dict[str, pd.DataFrame]]:
    """Run every KPI and aggregation, save tables as CSV, and return results."""

    # -- Scalar KPIs (single numbers) --
    metrics: Dict[str, float] = {
        "OEE": calc_oee(df),
        "FPY": calc_fpy(df),
        "DPU": calc_dpu(df),
        "COPQ": calc_copq(df),
        "Andon_Response_Time_sec": andon_response_time_metric(df),
    }

    # -- Summary tables --
    tables: Dict[str, pd.DataFrame] = {
        "defect_pareto": defect_pareto(df),
        "cycle_time_stats": cycle_time_distribution(df),
        "bottlenecks": identify_bottlenecks(df),
        "traceability": build_traceability_chain(df),
        "throughput_vs_wip": throughput_vs_wip(df),
        "pfmea": pfmea_risk_score(df),
        "energy_per_vehicle": energy_per_vehicle(df),
        "rework_loops": detect_rework_loops(df),
        "supplier_ppm": supplier_ppm(df),
        "tool_kpi": tool_calibration_kpi(df),
        "shift_performance": shift_performance(df),
        "warranty_early_warning": warranty_early_warning(df),
        "line_balance": line_balance_metric(df),
        "takt_time": takt_time_adherence(df),
        "plan_vs_actual": plan_vs_actual(df),
    }

    # Save each table as a CSV report
    for name, table in tables.items():
        path = get_report_path(f"{name}.csv")
        write_csv(table, path)

    return metrics, tables
