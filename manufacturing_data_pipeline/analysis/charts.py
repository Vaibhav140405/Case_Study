"""
Charts - generate visualizations and save them as PNG files.

Each function creates one chart and saves it under output/charts/.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from utils import get_output_chart_path, ensure_directories


sns.set(style="whitegrid")


def defect_pareto_chart(df: pd.DataFrame) -> None:
    """Bar chart showing which defect types happen most often."""

    counts = (
        df["Defect_Normalized"]
        .value_counts(dropna=False)
        .rename_axis("Defect")
        .reset_index(name="Count")
    )
    counts = counts.sort_values("Count", ascending=False)

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=counts, x="Defect", y="Count", ax=ax, color="steelblue")
    ax.set_title("Defect Pareto")
    ax.set_xlabel("Defect Category")
    ax.set_ylabel("Count")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    path = get_output_chart_path("defect_pareto.png")
    fig.savefig(path)
    plt.close(fig)


def cycle_time_histogram(df: pd.DataFrame) -> None:
    """Histogram showing how cycle times are spread out."""

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.histplot(
        pd.to_numeric(df["Cycle_Time"], errors="coerce").dropna(),
        bins=40,
        kde=False,
        ax=ax,
        color="teal",
    )
    ax.set_title("Cycle Time Distribution")
    ax.set_xlabel("Cycle Time (seconds)")
    ax.set_ylabel("Frequency")
    plt.tight_layout()
    path = get_output_chart_path("cycle_time_histogram.png")
    fig.savefig(path)
    plt.close(fig)


def supplier_defect_rate_chart(df: pd.DataFrame) -> None:
    """Bar chart of defect rate per supplier."""

    defective = df["Defect_Normalized"].isin(["Reject", "Repair"])
    grouped = (
        df.assign(Defective=defective)
        .groupby("Supplier", as_index=False)
        .agg(
            Total=("Event_ID", "count"),
            Defects=("Defective", "sum"),
        )
    )
    grouped["Defect_Rate"] = grouped["Defects"] / grouped["Total"].replace(0, 1)

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=grouped, x="Supplier", y="Defect_Rate", ax=ax, color="salmon")
    ax.set_title("Supplier Defect Rate")
    ax.set_ylabel("Defect Rate")
    plt.tight_layout()
    path = get_output_chart_path("supplier_defect_rate.png")
    fig.savefig(path)
    plt.close(fig)


def shift_productivity_chart(df: pd.DataFrame) -> None:
    """Bar chart comparing total output across shifts."""

    grouped = (
        df.groupby("Shift_Derived", as_index=False)["Actual_Output"]
        .sum()
        .rename(columns={"Actual_Output": "Total_Output"})
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=grouped, x="Shift_Derived", y="Total_Output", ax=ax, color="mediumseagreen")
    ax.set_title("Shift Productivity (Total Output)")
    ax.set_xlabel("Shift")
    ax.set_ylabel("Total Output")
    plt.tight_layout()
    path = get_output_chart_path("shift_productivity.png")
    fig.savefig(path)
    plt.close(fig)


def torque_spc_chart(df: pd.DataFrame) -> None:
    """SPC chart for torque - shows readings over time with control limits.

    The red dashed lines are the upper/lower control limits (3 sigma).
    Points outside those lines may indicate a problem.
    """

    df_sorted = df.sort_values("TS_dt")
    torque = df_sorted["Torque_Nm"]
    mean = torque.mean()
    std = torque.std()
    ucl = mean + 3 * std
    lcl = mean - 3 * std

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(torque.values, marker="o", linestyle="-", linewidth=0.7, markersize=2)
    ax.axhline(mean, color="green", linestyle="--", label="Mean")
    ax.axhline(ucl, color="red", linestyle="--", label="UCL (+3σ)")
    ax.axhline(lcl, color="red", linestyle="--", label="LCL (-3σ)")
    ax.set_title("Torque SPC Chart")
    ax.set_ylabel("Torque (Nm)")
    ax.set_xlabel("Event Sequence")
    ax.legend()
    plt.tight_layout()
    path = get_output_chart_path("torque_spc.png")
    fig.savefig(path)
    plt.close(fig)


def temperature_spc_chart(df: pd.DataFrame) -> None:
    """SPC chart for temperature - same idea as the torque chart."""

    df_sorted = df.sort_values("TS_dt")
    temp = df_sorted["Temp_C"]
    mean = temp.mean()
    std = temp.std()
    ucl = mean + 3 * std
    lcl = mean - 3 * std

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(temp.values, marker="o", linestyle="-", linewidth=0.7, markersize=2)
    ax.axhline(mean, color="green", linestyle="--", label="Mean")
    ax.axhline(ucl, color="red", linestyle="--", label="UCL (+3σ)")
    ax.axhline(lcl, color="red", linestyle="--", label="LCL (-3σ)")
    ax.set_title("Temperature SPC Chart")
    ax.set_ylabel("Temperature (°C)")
    ax.set_xlabel("Event Sequence")
    ax.legend()
    plt.tight_layout()
    path = get_output_chart_path("temperature_spc.png")
    fig.savefig(path)
    plt.close(fig)


def station_bottleneck_chart(bottlenecks: pd.DataFrame) -> None:
    """Bar chart of the top 15 slowest stations."""

    to_plot = bottlenecks.copy()
    to_plot = to_plot.sort_values("Avg_Cycle_Time", ascending=False).head(15)

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=to_plot, x="Station", y="Avg_Cycle_Time", ax=ax, color="slateblue")
    ax.set_title("Station Bottlenecks (Avg Cycle Time)")
    ax.set_xlabel("Station")
    ax.set_ylabel("Average Cycle Time (s)")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    path = get_output_chart_path("station_bottleneck.png")
    fig.savefig(path)
    plt.close(fig)


def generate_all_charts(df: pd.DataFrame, bottlenecks: pd.DataFrame) -> None:
    """Generate every chart and save them under output/charts/."""

    ensure_directories()
    defect_pareto_chart(df)
    cycle_time_histogram(df)
    supplier_defect_rate_chart(df)
    shift_productivity_chart(df)
    torque_spc_chart(df)
    temperature_spc_chart(df)
    station_bottleneck_chart(bottlenecks)
    print("Charts generated under output/charts/")
