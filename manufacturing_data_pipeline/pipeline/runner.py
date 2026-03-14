"""
Pipeline runner - ties all the steps together.

Order of execution:
  1. Generate messy raw data
  2. Clean and standardize
  3. Compute KPIs / transformations
  4. Generate charts
  5. Print detailed metrics to the console
"""

from __future__ import annotations

from typing import Dict, Any

import numpy as np
import pandas as pd

from utils import ensure_directories, write_csv, get_data_path
from data_generator import generate_messy_data
from pipeline.config import RAW_DATA_FILENAME
from cleaning import clean_raw_data
from transformations import run_all_metrics
from analysis import generate_all_charts


def _section(title: str) -> str:
    """Return a formatted section header for console output."""
    width = 60
    return f"\n{'=' * width}\n  {title}\n{'=' * width}"


def _print_detailed_metrics(
    raw_count: int,
    cleaned_df: pd.DataFrame,
    metrics: Dict[str, float],
    tables: Dict[str, pd.DataFrame],
) -> None:
    """Print a comprehensive, human-readable metrics report."""

    # ------------------------------------------------------------------
    # 1. DATA OVERVIEW
    # ------------------------------------------------------------------
    print(_section("DATA OVERVIEW"))
    print(f"  Raw rows generated        : {raw_count:,}")
    print(f"  Rows after cleaning        : {cleaned_df.shape[0]:,}")
    print(f"  Rows removed by cleaning   : {raw_count - cleaned_df.shape[0]:,}")
    pct_removed = (raw_count - cleaned_df.shape[0]) / max(raw_count, 1) * 100
    print(f"  Removal rate               : {pct_removed:.1f}%")
    print(f"  Final columns              : {cleaned_df.shape[1]}")

    # ------------------------------------------------------------------
    # 2. KEY PERFORMANCE INDICATORS (KPIs)
    # ------------------------------------------------------------------
    print(_section("KEY PERFORMANCE INDICATORS (KPIs)"))

    oee = metrics.get("OEE", float("nan"))
    fpy = metrics.get("FPY", float("nan"))
    dpu = metrics.get("DPU", float("nan"))
    copq = metrics.get("COPQ", float("nan"))
    andon_rt = metrics.get("Andon_Response_Time_sec", float("nan"))

    print(f"  OEE  (Overall Equipment Effectiveness) : {oee:.4f}  ({oee * 100:.2f}%)")
    print(f"       -> Measures how well equipment is used (availability x performance x quality).")
    print(f"  FPY  (First Pass Yield)                : {fpy:.4f}  ({fpy * 100:.2f}%)")
    print(f"       -> % of units that passed inspection without any rework.")
    print(f"  DPU  (Defects Per Unit)                : {dpu:.4f}  ({dpu * 100:.2f}%)")
    print(f"       -> Fraction of events that had a Reject or Repair defect.")
    print(f"  COPQ (Cost of Poor Quality)            : ${copq:,.2f}")
    print(f"       -> Estimated cost: $100/reject + $30/repair.")
    print(f"  Andon Response Time                    : {andon_rt:,.1f} seconds")
    print(f"       -> Average time between an Andon pull and the next event.")

    # ------------------------------------------------------------------
    # 3. DEFECT ANALYSIS
    # ------------------------------------------------------------------
    print(_section("DEFECT ANALYSIS"))

    defect_counts = cleaned_df["Defect_Normalized"].value_counts()
    total = len(cleaned_df)
    print(f"  {'Defect Type':<20} {'Count':>8} {'% of Total':>12}")
    print(f"  {'-' * 42}")
    for defect, count in defect_counts.items():
        pct = count / total * 100
        print(f"  {defect:<20} {count:>8,} {pct:>11.2f}%")

    reject_count = defect_counts.get("Reject", 0)
    repair_count = defect_counts.get("Repair", 0)
    print(f"\n  Total defective (Reject + Repair): {reject_count + repair_count:,} / {total:,}")

    # ------------------------------------------------------------------
    # 4. STATION PERFORMANCE
    # ------------------------------------------------------------------
    print(_section("STATION PERFORMANCE (by Avg Cycle Time)"))

    bottlenecks = tables.get("bottlenecks", pd.DataFrame())
    if not bottlenecks.empty:
        print(f"  {'Station':<12} {'Avg Cycle Time (s)':>20}")
        print(f"  {'-' * 34}")
        for idx, row in bottlenecks.iterrows():
            first = bottlenecks.index[0]
            marker = "  << slowest" if idx == first else ""
            print(f"  {row['Station']:<12} {row['Avg_Cycle_Time']:>20.2f}{marker}")

    # ------------------------------------------------------------------
    # 5. SHIFT PERFORMANCE
    # ------------------------------------------------------------------
    print(_section("SHIFT PERFORMANCE"))

    shift_df = tables.get("shift_performance", pd.DataFrame())
    if not shift_df.empty:
        print(f"  {'Shift':<12} {'Output':>10} {'Events':>10} {'FPY':>10}")
        print(f"  {'-' * 44}")
        for _, row in shift_df.iterrows():
            shift_fpy = row.get("FPY", float("nan"))
            print(
                f"  {row['Shift_Derived']:<12}"
                f" {int(row['Total_Output']):>10,}"
                f" {int(row['Events']):>10,}"
                f" {shift_fpy:>9.2%}"
            )

    # ------------------------------------------------------------------
    # 6. SUPPLIER QUALITY
    # ------------------------------------------------------------------
    print(_section("SUPPLIER QUALITY (Defective Parts Per Million)"))

    supplier_df = tables.get("supplier_ppm", pd.DataFrame())
    if not supplier_df.empty:
        print(f"  {'Supplier':<12} {'Total':>8} {'Defects':>10} {'PPM':>12}")
        print(f"  {'-' * 44}")
        for _, row in supplier_df.iterrows():
            print(
                f"  {row['Supplier']:<12}"
                f" {int(row['Total']):>8,}"
                f" {int(row['Defects']):>10,}"
                f" {row['PPM']:>12,.0f}"
            )

    # ------------------------------------------------------------------
    # 7. PLAN vs ACTUAL
    # ------------------------------------------------------------------
    print(_section("PLAN vs ACTUAL OUTPUT (by Line)"))

    pva_df = tables.get("plan_vs_actual", pd.DataFrame())
    if not pva_df.empty:
        print(f"  {'Line':<12} {'Planned':>10} {'Actual':>10} {'Adherence':>12}")
        print(f"  {'-' * 46}")
        for _, row in pva_df.iterrows():
            adh = row.get("Plan_Adherence", float("nan"))
            print(
                f"  {row['Line']:<12}"
                f" {int(row['Planned']):>10,}"
                f" {int(row['Actual']):>10,}"
                f" {adh:>11.2%}"
            )

    # ------------------------------------------------------------------
    # 8. ANOMALY SUMMARY
    # ------------------------------------------------------------------
    print(_section("ANOMALY & DATA QUALITY SUMMARY"))

    if "Anomaly_Flag" in cleaned_df.columns:
        anomaly_count = cleaned_df["Anomaly_Flag"].sum()
        print(f"  Sensor anomalies detected  : {int(anomaly_count):,} / {total:,} ({anomaly_count / total * 100:.2f}%)")
    if "Rework_Flag" in cleaned_df.columns:
        rework_count = cleaned_df["Rework_Flag"].sum()
        print(f"  Rework-flagged events      : {int(rework_count):,} / {total:,} ({rework_count / total * 100:.2f}%)")
    if "Maintenance_Flag" in cleaned_df.columns:
        maint_count = cleaned_df["Maintenance_Flag"].sum()
        print(f"  Maintenance events         : {int(maint_count):,} / {total:,} ({maint_count / total * 100:.2f}%)")
    if "Andon_Flag" in cleaned_df.columns:
        andon_count = cleaned_df["Andon_Flag"].sum()
        print(f"  Andon pulls                : {int(andon_count):,} / {total:,} ({andon_count / total * 100:.2f}%)")
    if "BOM_Valid" in cleaned_df.columns:
        bom_invalid = (~cleaned_df["BOM_Valid"]).sum()
        print(f"  BOM version invalid at time: {int(bom_invalid):,} / {total:,} ({bom_invalid / total * 100:.2f}%)")

    # ------------------------------------------------------------------
    # 9. OUTPUT FILES
    # ------------------------------------------------------------------
    print(_section("OUTPUT FILES GENERATED"))
    print("  Charts  -> output/charts/  (7 PNG files)")
    print("  Reports -> output/reports/ (15 CSV files)")
    print("  Data    -> data/cleaned_data.csv")
    print()


def run_pipeline() -> Dict[str, Any]:
    """Run the full data pipeline and return computed metrics."""

    ensure_directories()

    # Step 1 - generate a messy dataset (simulates raw factory data)
    raw_df = generate_messy_data()
    raw_path = get_data_path(RAW_DATA_FILENAME)
    write_csv(raw_df, raw_path)
    raw_count = len(raw_df)
    print(f"Dataset Generated: {raw_count:,} rows")

    # Step 2 - clean the dataset (fix formats, remove duplicates, etc.)
    cleaned_df = clean_raw_data()
    print("Cleaning Completed")

    # Step 3 - run transformations and compute KPIs
    metrics, tables = run_all_metrics(cleaned_df)

    # Step 4 - generate charts (uses the bottleneck table for one chart)
    bottlenecks = tables.get("bottlenecks", pd.DataFrame())
    generate_all_charts(cleaned_df, bottlenecks)

    # Step 5 - print the detailed metrics report
    _print_detailed_metrics(raw_count, cleaned_df, metrics, tables)

    return {"metrics": metrics, "tables": tables}
