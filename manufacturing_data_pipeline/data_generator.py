"""
Data generator for the manufacturing pipeline.

Creates a messy synthetic dataset used for demonstrating
data cleaning and transformation scenarios in the case study.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import List

import numpy as np
import pandas as pd

from utils import (
    ensure_directories,
    get_data_path,
    write_csv,
    default_random_seed,
)
from pipeline.config import RAW_DATA_FILENAME


# ----------------------------------------------------
# Timestamp generator
# ----------------------------------------------------

def _random_timestamp_strings(
    start: datetime,
    n: int,
    rng: np.random.Generator,
) -> List[str]:

    formats = [
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%d-%m-%Y %H:%M",
    ]

    timestamps = []

    for i in range(n):

        t = start + timedelta(seconds=i * 30)
        fmt = rng.choice(formats)

        ts = t.strftime(fmt)

        # Inject invalid minutes like 06:70
        if rng.random() < 0.02:
            ts = ts[:-2] + "70"

        timestamps.append(ts)

    return timestamps


# ----------------------------------------------------
# Helper to inject noisy categorical values
# ----------------------------------------------------

def _inject_category_noise(
    n: int,
    variants: List[List[str]],
) -> List[str]:

    result = []

    for _ in range(n):

        group = random.choice(variants)
        result.append(random.choice(group))

    return result


# ----------------------------------------------------
# Main generator
# ----------------------------------------------------

def generate_messy_data(
    n_rows: int = 7500,
    random_state: int | None = None,
) -> pd.DataFrame:

    if random_state is None:
        random_state = default_random_seed()

    rng = np.random.default_rng(random_state)
    random.seed(random_state)

    # ----------------------------------------
    # Category variants (messy formats)
    # ----------------------------------------

    line_variants = [
        ["ga-1", "GA-1", " Ga-1 "],
        ["ga-2", "GA-2", " Ga-2 "],
    ]

    station_variants = [
        [f"STN-{i:02d}", f"stn-{i:02d}", f" STN-{i:02d} "]
        for i in range(1, 8)
    ]

    inspection_variants = [
        ["human", "Human", "HUMAN"],
        ["AUTO", "Automated", "automated"],
    ]

    defect_variants = [
        ["None", "OK", "na", ""],
        ["Reject", "REJ"],
        ["Repair", "REP"],
    ]

    supplier_variants = [
        ["sup-01", "SUP-01"],
        ["sup-02", "SUP-02"],
        ["sup-09", "SUP-09"],
    ]

    tools = [f"TL-{i:03d}" for i in range(1, 25)]

    # ----------------------------------------
    # Generate timestamps
    # ----------------------------------------

    start = datetime(2026, 3, 10, 6, 0)

    ts_strings = _random_timestamp_strings(start, n_rows, rng)

    # ----------------------------------------
    # Part numbers (messy)
    # ----------------------------------------

    part_numbers = []

    for _ in range(n_rows):

        num = rng.integers(100, 999)
        alpha = random.choice(["ABC", "DEF", "GHI"])

        part_numbers.append(
            random.choice(
                [
                    f"{num}-{alpha}",
                    f"{num}{alpha}",
                    f"{num}-{alpha.lower()}",
                    f"{num}{alpha.lower()}",
                ]
            )
        )

    # ----------------------------------------
    # Sensor values
    # ----------------------------------------

    torque_base = rng.normal(50, 5, n_rows)
    temp_base = rng.normal(80, 10, n_rows)
    pressure_base = rng.normal(10, 1.5, n_rows)

    torque = [
        random.choice(
            [
                f"{round(v,1)}",
                f"{round(v,1)}Nm",
                f"{round(v,1)} Nm",
            ]
        )
        for v in torque_base
    ]

    temp = []

    for v in temp_base:

        if random.random() < 0.2:
            f = v * 9 / 5 + 32
            temp.append(f"{round(f,1)}F")
        else:
            temp.append(
                random.choice(
                    [
                        f"{round(v,1)}C",
                        f"{round(v,1)} c",
                        f"{round(v,1)}",
                    ]
                )
            )

    pressure = []

    for v in pressure_base:

        if random.random() < 0.3:
            psi = v * 14.5
            pressure.append(f"{round(psi,1)}psi")
        else:
            pressure.append(f"{round(v,1)} bar")

    cycle_time = rng.normal(60, 10, n_rows)

    # Inject outliers
    outliers = rng.choice(n_rows, 20)
    torque_base[outliers] = 1200

    # ----------------------------------------
    # Create dataframe (14 columns)
    # ----------------------------------------

    df = pd.DataFrame(
        {
            "Event_ID": range(1, n_rows + 1),

            "Line": _inject_category_noise(n_rows, line_variants),

            "Station": _inject_category_noise(n_rows, station_variants),

            "TS": ts_strings,

            "Part_No": part_numbers,

            "BOM_Version": rng.choice(
                ["BOM-1", "BOM-2", "BOM-3"],
                size=n_rows,
            ),

            "Torque": torque,

            "Temp": temp,

            "Pressure": pressure,

            "Cycle_Time": np.round(cycle_time, 2),

            "Defect": _inject_category_noise(n_rows, defect_variants),

            "Inspection_Source": _inject_category_noise(n_rows, inspection_variants),

            "Tool_ID": rng.choice(tools, size=n_rows),

            "Supplier": _inject_category_noise(n_rows, supplier_variants),
        }
    )

    # ----------------------------------------
    # Inject 6% NULL values
    # ----------------------------------------

    null_columns = ["TS", "Part_No", "Torque", "Temp", "Pressure", "Supplier"]

    for col in null_columns:

        idx = rng.choice(df.index, size=int(0.06 * n_rows), replace=False)

        df.loc[idx, col] = np.nan

    # ----------------------------------------
    # Inject duplicates (2%)
    # ----------------------------------------

    dup_rows = df.sample(int(n_rows * 0.02))

    df = pd.concat([df, dup_rows], ignore_index=True)

    return df


# ----------------------------------------------------
# Main entry
# ----------------------------------------------------

def main() -> None:

    ensure_directories()

    print("Generating messy manufacturing dataset...")

    df = generate_messy_data()

    path = get_data_path(RAW_DATA_FILENAME)

    write_csv(df, path)

    print(f"\nDataset generated: {len(df)} rows")
    print("Columns:", len(df.columns))
    print("\nMissing values:")
    print(df.isnull().sum())


if __name__ == "__main__":
    main()