from __future__ import annotations

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Line & Station
# ---------------------------------------------------------------------------

def standardize_line_station(df: pd.DataFrame) -> pd.DataFrame:
    """Trim spaces, remove internal spaces, and uppercase Line / Station codes."""

    df = df.copy()

    # Strip leading/trailing whitespace and make everything uppercase
    df["Line"] = df["Line"].astype(str).str.strip().str.upper()
    df["Station"] = df["Station"].astype(str).str.strip().str.upper()

    # Remove any spaces stuck in the middle (e.g. "LINE 01" -> "LINE01")
    df["Line"] = df["Line"].str.replace(" ", "", regex=False)
    df["Station"] = df["Station"].str.replace(" ", "", regex=False)

    return df


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------

def normalize_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()

    # Step 1 - fill blank timestamps with the value from the row above
    df["TS"] = df["TS"].ffill()

    # Step 2 - remove accidental space before colon (e.g. " :" -> ":")
    df["TS"] = df["TS"].astype(str).str.replace(" :", ":", regex=False)

    # Step 3 - split date and time, fix minutes >= 60 using vectorized math
    # e.g. "2026-03-10 06:70" -> date="2026-03-10", time="06:70"
    date_part = df["TS"].str.split(" ").str[0]
    time_part = df["TS"].str.split(" ").str[1]

    # Pull out the hour and minute as numbers
    hour = time_part.str.split(":").str[0].astype(int)
    minute = time_part.str.split(":").str[1].astype(int)

    # If minutes are 60 or more, roll the extra into the hour
    # e.g. 06:70 -> hour=07, minute=10
    hour = hour + (minute // 60)
    minute = minute % 60

    # Rebuild the timestamp string with corrected time
    df["TS"] = (
        date_part + " "
        + hour.astype(str).str.zfill(2) + ":"
        + minute.astype(str).str.zfill(2)
    )

    # Step 4 - parse the cleaned strings into real datetime objects
    df["TS"] = pd.to_datetime(df["TS"], errors="coerce", format="mixed", dayfirst=True)

    # Step 5 - for rows that still couldn't be parsed (NaT),
    # assign a default time based on their shift
    shift_map = {
        "Morning": "06:00:00",
        "Evening": "14:00:00",
        "Night":   "22:00:00",
    }
    mask = df["TS"].isna()
    if mask.any():
        df.loc[mask, "TS"] = pd.to_datetime(
            "2026-03-10 " + df.loc[mask, "Shift"].map(shift_map)
        )

    # Keep a TS_dt alias so downstream code that reads TS_dt still works
    df["TS_dt"] = df["TS"]

    return df


# ---------------------------------------------------------------------------
# Part numbers
# ---------------------------------------------------------------------------

def normalize_part_numbers(df: pd.DataFrame) -> pd.DataFrame:
    

    df = df.copy()

    # Clean up: trim whitespace and make uppercase
    df["Part_No"] = df["Part_No"].astype(str).str.strip().str.upper()

    # If a part number looks like "123ABC" (no hyphen), insert one -> "123-ABC"
    df["Part_No"] = df["Part_No"].str.replace(
        r'^(\d{3})([A-Z]{3})$', r'\1-\2', regex=True
    )

    # Check if the part number matches the expected "123-ABC" format
    df["Part_Format_Valid"] = df["Part_No"].str.match(r'^\d{3}-[A-Z]{3}$')

    # Mark which part numbers exist in our data (simple BOM check)
    bom_parts = df["Part_No"].unique()
    df["Part_In_BOM"] = df["Part_No"].isin(bom_parts)

    return df


# ---------------------------------------------------------------------------
# Defect labels
# ---------------------------------------------------------------------------

def normalize_defect(df: pd.DataFrame) -> pd.DataFrame:
    

    df = df.copy()

    # Make everything uppercase and trimmed so we can match consistently
    df["Defect"] = df["Defect"].astype(str).str.strip().str.upper()

    # Map short codes and alternate spellings to standard labels
    defect_map = {
        "REP":    "Repair",
        "REPAIR": "Repair",
        "REJ":    "Reject",
        "REJECT": "Reject",
        "NAN":    "None",   # pandas turns NaN to string "NAN" after astype(str)
        "NONE":   "None",
    }
    df["Defect"] = df["Defect"].replace(defect_map)

    # Keep a Defect_Normalized alias so downstream code still works
    df["Defect_Normalized"] = df["Defect"]

    return df


# ---------------------------------------------------------------------------
# Inspection source
# ---------------------------------------------------------------------------

def normalize_inspection_source(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    # Lowercase everything so "HUMAN", "Human", "human" all match the same key
    cleaned = df["Inspection_Source"].astype(str).str.strip().str.lower()

    # Map all known variations to standard labels
    inspection_map = {
        "human":     "Human",
        "manual":    "Human",
        "auto":      "Automated",
        "automated": "Automated",
        "machine":   "Automated",
    }
    df["Inspection_Source_Clean"] = cleaned.map(inspection_map)

    # If something didn't match any key, fill it with "Unknown"
    df["Inspection_Source_Clean"] = df["Inspection_Source_Clean"].fillna("Unknown")

    # Keep an alias so downstream code that reads Inspection_Source_Normalized still works
    df["Inspection_Source_Normalized"] = df["Inspection_Source_Clean"]

    return df


# ---------------------------------------------------------------------------
# Supplier codes
# ---------------------------------------------------------------------------

def normalize_supplier_codes(df: pd.DataFrame) -> pd.DataFrame:
    

    df = df.copy()

    # Make uppercase and trim whitespace
    df["Supplier"] = df["Supplier"].astype(str).str.strip().str.upper()

    # Pull out just the number part (e.g. "SUP-1" -> "1")
    sup_num = df["Supplier"].str.extract(r'(\d+)')[0].astype(float)

    # Rebuild as a standard code with zero-padding (e.g. 1 -> "SUP-01")
    df["Supplier"] = "SUP-" + sup_num.astype("Int64").astype(str).str.zfill(2)

    return df


# ---------------------------------------------------------------------------
# Tool IDs
# ---------------------------------------------------------------------------

def normalize_tool_ids(df: pd.DataFrame) -> pd.DataFrame:
    

    df = df.copy()

    # Clean up the tool ID text
    df["Tool_ID_Clean"] = df["Tool_ID"].astype(str).str.strip().str.upper()

    # Pull out just the number (e.g. "TL-007" -> 7)
    df["Tool_Number"] = df["Tool_ID"].str.extract(r'(\d+)').astype(int)

    # Check if the format is exactly "TL-" followed by 3 digits
    df["Tool_ID_Valid"] = df["Tool_ID_Clean"].str.match(r"^TL-\d{3}$")

    # Build a small reference table of all 30 valid tools
    tool_master = pd.DataFrame({
        "Tool_ID_Clean": [f"TL-{i:03d}" for i in range(1, 31)],
        "Tool_Type": "Torque Wrench",
        "Calibration_Status": "Valid",
    })

    # Join the tool master info onto our data
    df = df.merge(tool_master, on="Tool_ID_Clean", how="left")

    # Mark whether the tool was found in the master list
    df["Tool_Master_Match"] = df["Tool_Type"].notna()

    return df


# ---------------------------------------------------------------------------
# Scrap reasons
# ---------------------------------------------------------------------------

def map_scrap_reasons(df: pd.DataFrame) -> pd.DataFrame:
    

    df = df.copy()

    # Replace missing scrap reasons with a placeholder so they're not blank
    df["Scrap_Reason"] = df["Scrap_Reason"].fillna("Unknown")

    # Clean up formatting: trim whitespace and make Title Case
    # e.g. "torque out of spec" -> "Torque Out Of Spec"
    df["Scrap_Reason"] = (
        df["Scrap_Reason"]
        .astype(str)
        .str.strip()
        .str.title()
    )
    return df


# ---------------------------------------------------------------------------
# Shift derivation
# ---------------------------------------------------------------------------

def derive_shift(df: pd.DataFrame) -> pd.DataFrame:
    

    df = df.copy()

    # Get the hour from each timestamp
    hour = df["TS"].dt.hour

    # Use numpy's vectorized "select" to pick shifts based on hour ranges:
    #   06:00-13:59 -> Morning, 14:00-21:59 -> Evening, else -> Night
    df["Shift_Derived"] = np.select(
        [
            (hour >= 6) & (hour < 14),
            (hour >= 14) & (hour < 22),
        ],
        ["Morning", "Evening"],
        default="Night",
    )

    # If the original shift doesn't match the derived one, fix it
    shift_mismatch = df["Shift"] != df["Shift_Derived"]
    df.loc[shift_mismatch, "Shift"] = df.loc[shift_mismatch, "Shift_Derived"]

    return df


# ---------------------------------------------------------------------------
# Rework loop identification
# ---------------------------------------------------------------------------

def identify_rework_loops(df: pd.DataFrame) -> pd.DataFrame:
    

    df = df.copy()

    # Count how many times each VIN visited each station
    station_visits = df.groupby(["VIN", "Station"]).size()

    # Keep only the cases where a VIN visited a station more than once
    rework = station_visits[station_visits > 1]

    # Get the list of VINs that had any rework
    rework_vins = rework.index.get_level_values("VIN").unique()

    # Tag every row belonging to a reworked VIN
    df["Rework_Flag"] = df["VIN"].isin(rework_vins)

    return df


# ---------------------------------------------------------------------------
# Work order linkage validation
# ---------------------------------------------------------------------------

def validate_work_orders(df: pd.DataFrame) -> pd.DataFrame:
   
    df = df.copy()

    # Count how many different work orders each VIN has
    wo_counts = df.groupby("VIN")["WO"].nunique()

    # Flag VINs that have more than one work order
    bad_vins = wo_counts[wo_counts > 1].index
    df["WO_Mismatch"] = df["VIN"].isin(bad_vins)

    return df


# ---------------------------------------------------------------------------
# BOM version effectivity checks
# ---------------------------------------------------------------------------

def validate_bom_version(df: pd.DataFrame) -> pd.DataFrame:
   
    df = df.copy()

    # Build a small lookup table of BOM versions and when they became active
    bom_master = pd.DataFrame({
        "BOM_Version": ["BOM-1", "BOM-2", "BOM-3"],
        "Effective_Date": ["2025-01-01", "2026-01-01", "2026-06-01"],
    })
    bom_master["Effective_Date"] = pd.to_datetime(bom_master["Effective_Date"])

    # Join the effective date onto each row based on its BOM version
    df = df.merge(bom_master, on="BOM_Version", how="left")

    # Make sure TS is datetime so we can compare dates
    df["TS"] = pd.to_datetime(df["TS"], errors="coerce")

    # Was the event on or after the BOM effective date?
    df["BOM_Valid"] = df["TS"] >= df["Effective_Date"]

    return df


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def remove_duplicate_events(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()

    # Pass 1 - remove rows that are 100% identical across all columns
    df = df.drop_duplicates()

    # Pass 2 - remove logical duplicates (same vehicle, station, and time)
    df["Is_Duplicate"] = df.duplicated(
        subset=["VIN", "Station", "TS"], keep="first"
    )
    df = df[~df["Is_Duplicate"]].copy()

    return df


# ---------------------------------------------------------------------------
# Missing values
# ---------------------------------------------------------------------------

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()

    # Turn empty strings and single spaces into proper NaN values
    df.replace({"": np.nan, " ": np.nan}, inplace=True)

    # For these columns, 'Unknown' is a better default than a blank cell
    fill_cols = [
        "Defect_Normalized",              # cleaned defect label
        "Inspection_Source_Normalized",    # cleaned inspection source
        "Inspection_Source_Clean",         # notebook-style column
        "Scrap_Reason",                    # scrap reason (notebook fills with 'Unknown')
        "Defect_Code",                     # defect code can also be blank
        "Inspection_Result",               # inspection result can be blank
    ]
    for col in fill_cols:
        if col in df:
            df[col] = df[col].fillna("Unknown")

    return df


# ---------------------------------------------------------------------------
# Outlier / sensor drift detection
# ---------------------------------------------------------------------------

def detect_sensor_drift(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()

    # Sort so each machine's readings are in time order
    df = df.sort_values(["Machine_ID", "TS"])

    # Compute a rolling average of torque for each machine (window = 50 readings)
    rolling_mean = (
        df.groupby("Machine_ID")["Torque_Nm"]
        .rolling(50)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # Check if the rolling average jumped by more than 5 Nm
    drift = rolling_mean.diff().abs() > 5

    # Store the flag (fill NaN with False for the first rows that have no history)
    df["Sensor_Drift_Flag"] = drift.fillna(False)

    return df


# ---------------------------------------------------------------------------
# Unit conversion - torque
# ---------------------------------------------------------------------------

def convert_torque_to_nm(df: pd.DataFrame) -> pd.DataFrame:
    

    df = df.copy()

    # Uppercase so we can spot "LB" or "NM" easily
    torque_str = df["Torque"].astype(str).str.strip().str.upper()

    # Pull out just the number (e.g. "50.5 Nm" -> "50.5")
    torque_val = torque_str.str.extract(r'(\d+\.?\d*)')[0].astype(float)

    # Find rows that are in lb-ft (contain "LB" in the text)
    is_lbft = torque_str.str.contains("LB", na=False)

    # Convert lb-ft to Nm (1 lb-ft = 1.35582 Nm)
    torque_val.loc[is_lbft] = torque_val.loc[is_lbft] * 1.35582

    # Store the cleaned numeric values
    df["Torque"] = torque_val
    df["Torque_Nm"] = torque_val

    return df


# ---------------------------------------------------------------------------
# Unit conversion - temperature
# ---------------------------------------------------------------------------

def convert_temp_to_c(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()

    # Uppercase so we can spot "C" or "F" easily
    temp_str = df["Temp"].astype(str).str.strip().str.upper()

    # Pull out just the number
    temp_val = temp_str.str.extract(r'(\d+\.?\d*)')[0].astype(float)

    # Pull out the unit letter (C or F)
    temp_unit = temp_str.str.extract(r'([CF])')[0]

    # Overwrite the Temp column with just the number
    df["Temp"] = temp_val

    # Start with the numeric value as-is (assume Celsius by default)
    df["Temp_C"] = temp_val

    # Where the unit was F, convert to Celsius: (F - 32) * 5/9
    is_fahrenheit = temp_unit == "F"
    df.loc[is_fahrenheit, "Temp_C"] = (temp_val[is_fahrenheit] - 32) * 5 / 9

    return df


# ---------------------------------------------------------------------------
# Unit conversion - pressure
# ---------------------------------------------------------------------------

def convert_pressure_to_bar(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()

    # Uppercase so we can spot "PSI" easily
    pressure_str = df["Pressure"].astype(str).str.strip().str.upper()

    # Pull out just the number
    pressure_val = pressure_str.str.extract(r'(\d+\.?\d*)')[0].astype(float)

    # Find rows that are in PSI
    is_psi = pressure_str.str.contains("PSI", na=False)

    # Convert PSI to bar (1 bar = 14.5038 PSI)
    pressure_val.loc[is_psi] = pressure_val.loc[is_psi] / 14.5038

    # Store the cleaned numeric values
    df["Pressure"] = pressure_val
    df["Pressure_bar"] = pressure_val

    return df

# ---------------------------------------------------------------------------
# Calibration factors
# ---------------------------------------------------------------------------

def apply_calibration_factors(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Convert the calibration factor column to a number
    factor = df["Calibration_Factor"].astype(float)

    # Apply the factor to each sensor reading
    df["Torque"] = df["Torque"] * factor
    df["Torque_Nm"] = df["Torque"]

    df["Temp"] = df["Temp"] * factor
    df["Temp_C"] = df["Temp"]

    df["Pressure"] = df["Pressure"] * factor
    df["Pressure_bar"] = df["Pressure"]

    return df
