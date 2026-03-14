"""
Cleaning package - runs all data cleaning steps on raw manufacturing data.

Cleaning logic is split across two files:
  - normalization.py : all normalization, deduplication, missing values,
                       outlier detection, and unit conversion steps
  - validation.py    : check VINs, filter bad rows, flag anomalies

This file ties them all together in clean_raw_data().
The order follows the notebook (data_cleaning_new.ipynb) exactly.
"""

from __future__ import annotations

import pandas as pd

# -- Import every cleaning step --

from cleaning.normalization import (
    standardize_line_station,
    normalize_timestamps,
    normalize_part_numbers,
    normalize_defect,
    normalize_inspection_source,
    normalize_supplier_codes,
    normalize_tool_ids,
    map_scrap_reasons,
    derive_shift,
    identify_rework_loops,
    validate_work_orders,
    validate_bom_version,
    remove_duplicate_events,
    handle_missing_values,
    detect_sensor_drift,
    convert_torque_to_nm,
    convert_temp_to_c,
    convert_pressure_to_bar,
    apply_calibration_factors,
)
from cleaning.validation import validate_vin, cycle_time_plausibility, flag_anomalies, mask_vin_for_export

from utils import get_data_path, write_csv, ensure_directories, read_csv
from pipeline.config import RAW_DATA_FILENAME, CLEANED_DATA_FILENAME


def clean_raw_data() -> pd.DataFrame:
    """Run the full cleaning pipeline on raw_data.csv and save the result.

    The order matches the notebook (data_cleaning_new.ipynb):
      1. Remove exact duplicate rows
      2. Normalize Line & Station text
      3. Fix and parse timestamps, fill NaTs from shift
      4. Normalize part numbers and validate format
      5. Convert torque to Nm (including lb-ft -> Nm)
      6. Convert temperature to Celsius (including F -> C)
      7. Normalize defect labels
      8. Validate VINs, remove invalid, mask for PII
      9. Normalize supplier codes
     10. Remove logical duplicates (VIN + Station + TS)
     11. Convert pressure to bar (including PSI -> bar)
     12. Apply sensor calibration factors
     13. Detect sensor drift (rolling torque check)
     14. Check cycle time plausibility (remove implausible gaps)
     15. Validate work orders (VIN -> WO linkage)
     16. Identify rework loops (repeated station visits)
     17. Derive shift from timestamp and fix mismatches
     18. Map scrap reasons (fill blanks with 'Unknown', title-case)
     19. Normalize inspection source (Human / Automated)
     20. Normalize tool IDs and merge tool master
     21. Handle remaining missing values
     22. Flag sensor anomalies (out-of-spec readings)
     23. Validate BOM version effectivity dates
    """

    ensure_directories()
    raw_path = get_data_path(RAW_DATA_FILENAME)
    df = read_csv(raw_path)

    # Step 1 - remove rows that are 100% identical (exact dupes)
    df = remove_duplicate_events(df)

    # Step 2 - clean up Line and Station text (uppercase, remove spaces)
    df = standardize_line_station(df)

    # Step 3 - fix messy timestamps and parse into datetime
    df = normalize_timestamps(df)

    # Step 4 - clean part numbers (uppercase, add hyphen if missing)
    df = normalize_part_numbers(df)

    # Step 5 - parse torque values, convert lb-ft to Nm
    df = convert_torque_to_nm(df)

    # Step 6 - parse temperature values, convert Fahrenheit to Celsius
    df = convert_temp_to_c(df)

    # Step 7 - normalize defect labels (Repair / Reject / None)
    df = normalize_defect(df)

    # Step 8 - validate VINs, remove invalid rows, mask for privacy
    df = validate_vin(df)

    # Step 9 - normalize supplier codes to SUP-XX format
    df = normalize_supplier_codes(df)

    # Step 10 - convert pressure values, convert PSI to bar
    df = convert_pressure_to_bar(df)

    # Step 11 - apply sensor calibration correction factors
    df = apply_calibration_factors(df)

    # Step 12 - detect torque sensor drift (rolling average check)
    df = detect_sensor_drift(df)

    # Step 13 - check cycle time plausibility and remove bad rows
    df = cycle_time_plausibility(df)

    # Step 14 - check that each VIN maps to exactly one work order
    df = validate_work_orders(df)

    # Step 15 - tag vehicles that went back to the same station (rework)
    df = identify_rework_loops(df)

    # Step 16 - derive shift from timestamp and fix mismatches
    df = derive_shift(df)

    # Step 17 - fill blank scrap reasons with 'Unknown', clean formatting
    df = map_scrap_reasons(df)

    # Step 18 - standardize inspection source (Human / Automated)
    df = normalize_inspection_source(df)

    # Step 19 - clean tool IDs, validate format, merge tool master
    df = normalize_tool_ids(df)

    # Step 20 - fill remaining missing values in key columns
    df = handle_missing_values(df)

    # Step 21 - flag out-of-spec sensor readings (anomalies)
    df = flag_anomalies(df)

    # Step 22 - validate BOM version effectivity dates
    df = validate_bom_version(df)

    # Step 23 - mask VINs for privacy (must be the LAST step
    # because earlier steps like rework detection need the real VIN)
    df = mask_vin_for_export(df)

    # Step 24 - drop columns that are no longer needed after cleaning
    cols_to_drop = [
        # Original messy columns replaced by cleaned versions
        "Torque",              # -> Torque_Nm
        "Temp",                # -> Temp_C
        "Pressure",            # -> Pressure_bar / Pressure_Bar
        "TS",                  # -> TS_dt
        "Shift",               # -> Shift_Derived
        "Defect",              # -> Defect_Normalized
        "Inspection_Source",   # -> Inspection_Source_Normalized
        # Intermediate validation / cleaning flags
        "Is_Duplicate",
        "Part_Format_Valid",
        "Part_In_BOM",
        "VIN_IsValid",
        "Sensor_Drift_Flag",
        "WO_Mismatch",
        "Tool_ID_Clean",
        "Tool_Number",
        "Tool_ID_Valid",
        "Tool_Type",
        "Calibration_Status",
        "Tool_Master_Match",
        "Inspection_Source_Clean",
        # Intermediate BOM merge column (BOM_Valid is kept for reporting)
        "Effective_Date",
        # Duplicate pressure columns (both variants, Torque_Nm / Temp_C are kept)
        "Pressure_bar",
        "Pressure_Bar",
        # Individual anomaly sub-flags (already aggregated into Anomaly_Flag)
        "Torque_Anomaly",
        "Temp_Anomaly",
        "Pressure_Anomaly",
        "Cycle_Anomaly",
        "Sensor_Anomaly",
        # Raw sensor columns not used by any downstream transformation
        "Vibration",
        "Humidity",
        "Voltage",
        "Current",
        # Admin / reference columns not needed after cleaning
        "Batch_ID",
        "Production_Order",
        "Material_Type",
        "Assembly_Step",
        # Status / quality columns not consumed by transformations
        "Sensor_Status",
        "Quality_Score",
        # Calibration factor no longer needed after apply_calibration_factors
        "Calibration_Factor",
    ]
    existing = [c for c in cols_to_drop if c in df.columns]
    df.drop(columns=existing, inplace=True)

    # Print the transformed result
    print(f"\nTransformed Data: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"Columns: {list(df.columns)}")
    print(f"\nDropped {len(existing)} unnecessary columns: {existing}")
    print("\n--- First 5 rows of transformed data ---")
    print(df.head().to_string())

    # Save the cleaned data to a CSV file
    cleaned_path = get_data_path(CLEANED_DATA_FILENAME)
    write_csv(df, cleaned_path)
    return df
