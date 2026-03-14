"""
Aggregations - group and summarize data into useful tables.

Each function takes cleaned data and returns a small DataFrame
that can be saved as a CSV report or used for charts.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Defects
# ---------------------------------------------------------------------------

def defect_pareto(df: pd.DataFrame) -> pd.DataFrame:
    """Pareto table of defects - which defect types happen most often."""

    counts = (
        df["Defect_Normalized"]
        .value_counts(dropna=False)
        .rename_axis("Defect")
        .reset_index(name="Count")
    )
    counts["Pct"] = counts["Count"] / counts["Count"].sum()
    counts["CumPct"] = counts["Pct"].cumsum()
    return counts


# ---------------------------------------------------------------------------
# Cycle time
# ---------------------------------------------------------------------------

def cycle_time_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Basic statistics for cycle time (mean, std, quartiles)."""

    ct = pd.to_numeric(df["Cycle_Time"], errors="coerce")
    return pd.DataFrame(
        {
            "mean": [ct.mean()],
            "std": [ct.std()],
            "p25": [ct.quantile(0.25)],
            "p50": [ct.quantile(0.5)],
            "p75": [ct.quantile(0.75)],
        }
    )


# ---------------------------------------------------------------------------
# Bottlenecks
# ---------------------------------------------------------------------------

def identify_bottlenecks(df: pd.DataFrame) -> pd.DataFrame:
    """Find the slowest stations by average cycle time."""

    ct = pd.to_numeric(df["Cycle_Time"], errors="coerce")
    by_station = (
        df.assign(Cycle_Time=ct)
        .groupby("Station", as_index=False)["Cycle_Time"]
        .mean()
        .rename(columns={"Cycle_Time": "Avg_Cycle_Time"})
        .sort_values("Avg_Cycle_Time", ascending=False)
    )
    return by_station


# ---------------------------------------------------------------------------
# Traceability
# ---------------------------------------------------------------------------

def build_traceability_chain(df: pd.DataFrame) -> pd.DataFrame:
    """Build a simple chain: Supplier -> Lot -> Part -> VIN."""

    cols = ["Supplier", "Supplier_Lot", "Part_No", "VIN"]
    chain = df[cols].drop_duplicates()
    return chain


# ---------------------------------------------------------------------------
# Throughput vs WIP
# ---------------------------------------------------------------------------

def throughput_vs_wip(df: pd.DataFrame) -> pd.DataFrame:
    """Compare throughput (output) to work-in-progress (event count) per line."""

    grouped = (
        df.groupby("Line", as_index=False)
        .agg(
            Total_Output=("Actual_Output", "sum"),
            Total_Target=("Production_Target", "sum"),
            Events=("Event_ID", "count"),
        )
    )
    grouped["WIP_Proxy"] = grouped["Events"]
    grouped["Throughput_Ratio"] = grouped["Total_Output"] / grouped["Total_Target"].replace(0, np.nan)
    return grouped


# ---------------------------------------------------------------------------
# PFMEA risk score
# ---------------------------------------------------------------------------

def pfmea_risk_score(df: pd.DataFrame) -> pd.DataFrame:
    """Simple risk score per defect code (like a mini PFMEA).

    RPN = Severity x Occurrence x Detection
    Higher RPN = higher risk.
    """

    severity_map = {
        "TORQ_OOS": 8,
        "TEMP_OOS": 7,
        "LEAK": 9,
        "VISUAL_DEFECT": 5,
        "OTHER": 4,
    }
    detection_map = {
        "Human": 4,
        "Automated": 2,
        "Unknown": 5,
    }

    df = df.copy()
    df["Defect_Code_Clean"] = df["Defect_Code"].fillna("").replace("", "OTHER")
    df["Severity"] = df["Defect_Code_Clean"].map(severity_map).fillna(3)
    df["Occurrence"] = 1  # one per event
    df["Detection"] = df["Inspection_Source_Normalized"].map(detection_map).fillna(4)
    df["RPN"] = df["Severity"] * df["Occurrence"] * df["Detection"]

    by_code = (
        df.groupby("Defect_Code_Clean", as_index=False)
        .agg(
            Events=("Event_ID", "count"),
            Total_RPN=("RPN", "sum"),
        )
        .sort_values("Total_RPN", ascending=False)
    )
    return by_code


# ---------------------------------------------------------------------------
# Energy
# ---------------------------------------------------------------------------

def energy_per_vehicle(df: pd.DataFrame) -> pd.DataFrame:
    """Total energy consumed per vehicle (VIN)."""

    by_vin = (
        df.groupby("VIN", as_index=False)["Energy_Consumption"]
        .sum()
        .rename(columns={"Energy_Consumption": "Total_Energy"})
    )
    return by_vin


# ---------------------------------------------------------------------------
# Rework loops
# ---------------------------------------------------------------------------

def detect_rework_loops(df: pd.DataFrame) -> pd.DataFrame:
    """Find vehicles that went through rework more than once."""

    by_vin = (
        df.groupby("VIN", as_index=False)["Rework_Flag"]
        .sum()
        .rename(columns={"Rework_Flag": "Rework_Count"})
    )
    by_vin["Has_Rework_Loop"] = by_vin["Rework_Count"] > 0
    return by_vin


# ---------------------------------------------------------------------------
# Supplier PPM
# ---------------------------------------------------------------------------

def supplier_ppm(df: pd.DataFrame) -> pd.DataFrame:
    """Defective parts per million for each supplier."""

    defective = df["Defect_Normalized"].isin(["Reject", "Repair"])
    grouped = (
        df.assign(Defective=defective)
        .groupby("Supplier", as_index=False)
        .agg(
            Total=("Event_ID", "count"),
            Defects=("Defective", "sum"),
        )
    )
    grouped["PPM"] = grouped["Defects"] / grouped["Total"].replace(0, np.nan) * 1_000_000
    return grouped


# ---------------------------------------------------------------------------
# Tool calibration
# ---------------------------------------------------------------------------

def tool_calibration_kpi(df: pd.DataFrame) -> pd.DataFrame:
    """How far each tool's average torque deviates from the overall mean."""

    df = df.copy()
    overall_mean = df["Torque_Nm"].mean()
    by_tool = (
        df.groupby("Tool_ID", as_index=False)["Torque_Nm"]
        .mean()
        .rename(columns={"Torque_Nm": "Avg_Torque_Nm"})
    )
    by_tool["Torque_Deviation"] = by_tool["Avg_Torque_Nm"] - overall_mean
    return by_tool


# ---------------------------------------------------------------------------
# Shift performance
# ---------------------------------------------------------------------------

def shift_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Compare Morning / Evening / Night shifts by output and FPY."""

    df = df.copy()
    df["Is_Good_First_Pass"] = (df["Defect_Normalized"] == "None") & (df["Rework_Flag"] == 0)

    grouped = (
        df.groupby("Shift_Derived", as_index=False)
        .agg(
            Total_Output=("Actual_Output", "sum"),
            Events=("Event_ID", "count"),
            Good_First_Pass=("Is_Good_First_Pass", "sum"),
        )
    )
    grouped["FPY"] = grouped["Good_First_Pass"] / grouped["Events"].replace(0, np.nan)
    return grouped


# ---------------------------------------------------------------------------
# Warranty early warning
# ---------------------------------------------------------------------------

def warranty_early_warning(df: pd.DataFrame) -> pd.DataFrame:
    """Flag part + defect combos that might cause warranty claims."""

    df = df.copy()
    grouped = (
        df.groupby(["Part_No", "Defect_Code"], as_index=False)
        .agg(
            Events=("Event_ID", "count"),
            Warranty_Flags=("Warranty_Flag", "sum"),
        )
        .sort_values(["Warranty_Flags", "Events"], ascending=False)
    )
    return grouped


# ---------------------------------------------------------------------------
# Line balance
# ---------------------------------------------------------------------------

def line_balance_metric(df: pd.DataFrame) -> pd.DataFrame:
    """How evenly work is spread across stations (Balance Index).

    A Balance_Index near 1.0 means the station is close to the average.
    Much higher than 1.0 means it's a bottleneck.
    """

    by_station = identify_bottlenecks(df)
    if by_station.empty:
        return by_station

    avg = by_station["Avg_Cycle_Time"].mean()
    by_station["Balance_Index"] = by_station["Avg_Cycle_Time"] / avg
    return by_station


# ---------------------------------------------------------------------------
# Takt time
# ---------------------------------------------------------------------------

def takt_time_adherence(df: pd.DataFrame) -> pd.DataFrame:
    """Are we hitting the takt time (the pace we need to meet the production target)?"""

    df = df.copy()
    ct = pd.to_numeric(df["Cycle_Time"], errors="coerce")
    df["Cycle_Time"] = ct
    # Simple takt: available time (4800 sec) / target units
    df["Takt_Time"] = 4800 / df["Production_Target"].replace(0, np.nan)
    df["Takt_Adherence"] = df["Cycle_Time"] <= df["Takt_Time"]

    grouped = (
        df.groupby("Line", as_index=False)
        .agg(
            Adherence_Rate=("Takt_Adherence", "mean"),
            Avg_Takt=("Takt_Time", "mean"),
            Avg_Cycle_Time=("Cycle_Time", "mean"),
        )
    )
    return grouped


# ---------------------------------------------------------------------------
# Plan vs Actual
# ---------------------------------------------------------------------------

def plan_vs_actual(df: pd.DataFrame) -> pd.DataFrame:
    """Compare what was planned vs what was actually produced, per line."""

    grouped = (
        df.groupby("Line", as_index=False)
        .agg(
            Planned=("Production_Target", "sum"),
            Actual=("Actual_Output", "sum"),
        )
    )
    grouped["Plan_Adherence"] = grouped["Actual"] / grouped["Planned"].replace(0, np.nan)
    return grouped
