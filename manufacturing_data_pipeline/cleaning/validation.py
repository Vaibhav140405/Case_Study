"""
Validation - check data quality, filter bad rows, and flag anomalies.

This module:
  - Validates VINs and REMOVES invalid ones (matching the notebook's logic).
  - Masks VINs for privacy (PII protection) before export.
  - Checks cycle time plausibility using time deltas between events.
  - Flags out-of-spec sensor readings with individual + combined anomaly columns.

All functions use vectorized pandas operations (no .apply() or .iterrows()) for speed.
"""

from __future__ import annotations

import pandas as pd


# ---- Acceptable manufacturing ranges (from the notebook) ----
TORQUE_MIN = 20
TORQUE_MAX = 80
TEMP_MIN = 10
TEMP_MAX = 120
PRESSURE_MIN = 1
PRESSURE_MAX = 15
CYCLE_MIN = 5
CYCLE_MAX = 300


def validate_vin(df: pd.DataFrame) -> pd.DataFrame:
    """Validate VINs, remove invalid rows, and mask VINs for privacy.

    A proper VIN has exactly 17 characters, only letters A-Z (except I, O, Q)
    and digits 0-9.

    Steps:
      1. Trim whitespace and uppercase every VIN.
      2. Check format using a regex pattern (vectorized, no .apply()).
      3. REMOVE rows with invalid VINs (they are garbage data).
      4. Mask VINs for PII: keep first 3 and last 3 chars, hide the rest.
    """

    df = df.copy()

    # Clean up the VIN text
    df["VIN"] = df["VIN"].astype(str).str.strip().str.upper()

    # Check format: exactly 17 chars, only A-Z (no I,O,Q) and 0-9
    df["VIN_IsValid"] = df["VIN"].str.match(r'^[A-HJ-NPR-Z0-9]{17}$')

    # Keep only rows with valid VINs (drop the bad ones)
    df = df[df["VIN_IsValid"]].copy()

    return df


def mask_vin_for_export(df: pd.DataFrame) -> pd.DataFrame:
    """Mask VINs for privacy before exporting the data.

    Replaces the middle 11 characters with asterisks so the full VIN
    is not stored in the output file.  e.g. "ABC12345678901XYZ" -> "ABC***********XYZ"

    This must run AFTER all processing that groups by VIN is done.
    """

    df = df.copy()
    df["VIN"] = df["VIN"].astype(str).str[:3] + "***********" + df["VIN"].astype(str).str[-3:]
    return df


def cycle_time_plausibility(df: pd.DataFrame) -> pd.DataFrame:
    """Check cycle times by computing time gaps between events at each station.

    Steps:
      1. Sort events by Station then Timestamp.
      2. Compute the time difference (in seconds) between consecutive events
         at the same station.
      3. Remove rows where the gap is zero/negative or way too long (> 1800 sec).
      4. Reset the index so row numbers are clean.
    """

    df = df.copy()

    # Sort so events at the same station are in time order
    df = df.sort_values(["Station", "TS"])

    # Compute how many seconds passed between consecutive events at each station
    cycle_time = df.groupby("Station")["TS"].diff().dt.total_seconds()

    # Mark implausible gaps: zero/negative (impossible) or > 30 minutes (too long)
    implausible = (cycle_time <= 0) | (cycle_time > 1800)

    # Remove those bad rows
    df = df.loc[~implausible]
    df = df.reset_index(drop=True)

    return df


def flag_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Flag rows where sensor readings are outside acceptable ranges.

    Creates individual True/False flags for each sensor, plus a combined
    'Sensor_Anomaly' flag that is True if ANY sensor is out of range.

    Ranges (from the notebook):
      - Torque:   20 - 80 Nm
      - Temp:     10 - 120 C
      - Pressure:  1 - 15 bar
      - Cycle:     5 - 300 seconds

    Uses vectorized comparisons (no .iterrows()) for speed.
    """

    df = df.copy()

    # --- Create the Torque_Nm and Pressure_Bar columns used for anomaly checks ---
    # (strip any leftover text units and convert to numbers)
    df["Torque_Nm"] = (
        df["Torque"].astype(str)
        .str.replace("Nm", "", regex=False)
        .str.replace("nm", "", regex=False)
        .str.strip()
    )
    df["Torque_Nm"] = pd.to_numeric(df["Torque_Nm"], errors="coerce")

    df["Pressure_Bar"] = (
        df["Pressure"].astype(str)
        .str.replace("bar", "", regex=False)
        .str.replace("Bar", "", regex=False)
        .str.strip()
    )
    df["Pressure_Bar"] = pd.to_numeric(df["Pressure_Bar"], errors="coerce")

    # Make sure Cycle_Time is numeric
    df["Cycle_Time"] = pd.to_numeric(df["Cycle_Time"], errors="coerce")

    # --- Flag each sensor individually ---

    # Torque out of range?
    df["Torque_Anomaly"] = (
        (df["Torque_Nm"] < TORQUE_MIN) | (df["Torque_Nm"] > TORQUE_MAX)
    )

    # Temperature out of range?
    df["Temp_Anomaly"] = (
        (df["Temp_C"] < TEMP_MIN) | (df["Temp_C"] > TEMP_MAX)
    )

    # Pressure out of range?
    df["Pressure_Anomaly"] = (
        (df["Pressure_Bar"] < PRESSURE_MIN) | (df["Pressure_Bar"] > PRESSURE_MAX)
    )

    # Cycle time out of range?
    df["Cycle_Anomaly"] = (
        (df["Cycle_Time"] < CYCLE_MIN) | (df["Cycle_Time"] > CYCLE_MAX)
    )

    # --- Combined flag: True if ANY of the above is True ---
    df["Sensor_Anomaly"] = (
        df["Torque_Anomaly"]
        | df["Temp_Anomaly"]
        | df["Pressure_Anomaly"]
        | df["Cycle_Anomaly"]
    )

    # Keep backward-compat column names used by downstream code
    df["Anomaly_Flag"] = df["Sensor_Anomaly"]

    return df
