import pandas as pd
import numpy as np
import re


# ------------------------------
# 1. Load Data
# ------------------------------
def load_data(path):
    df = pd.read_csv(path)
    df = df.drop_duplicates()
    return df


# ------------------------------
# 2. Clean Line and Station
# ------------------------------
def clean_line_station(df):
    df['Line'] = df['Line'].astype(str).str.strip().str.upper().str.replace(" ", "")
    df['Station'] = df['Station'].astype(str).str.strip().str.upper().str.replace(" ", "")
    return df


# ------------------------------
# 3. Clean Timestamp
# ------------------------------
def clean_timestamp(df):

    df["TS"] = df["TS"].ffill()
    df["TS"] = df["TS"].str.replace(" :", ":", regex=False)

    time = df["TS"].str.split(" ").str[1]

    hour = time.str.split(":").str[0].astype(int)
    minute = time.str.split(":").str[1].astype(int)

    hour = hour + (minute // 60)
    minute = minute % 60

    df["TS"] = (
        df["TS"].str.split(" ").str[0] + " "
        + hour.astype(str).str.zfill(2) + ":"
        + minute.astype(str).str.zfill(2)
    )

    df["TS"] = pd.to_datetime(df["TS"], errors="coerce", format="mixed", dayfirst=True)

    shift_map = {
        "Morning": "06:00:00",
        "Evening": "14:00:00",
        "Night": "22:00:00"
    }

    mask = df["TS"].isna()

    df.loc[mask, "TS"] = pd.to_datetime(
        "2026-03-10 " + df.loc[mask, "Shift"].map(shift_map)
    )

    return df


# ------------------------------
# 4. Normalize Part Numbers
# ------------------------------
def normalize_part_numbers(df):

    df["Part_No"] = df["Part_No"].astype(str).str.strip().str.upper()

    df["Part_No"] = df["Part_No"].str.replace(
        r'^(\d{3})([A-Z]{3})$', r'\1-\2', regex=True
    )

    return df


# ------------------------------
# 5. Parse Torque
# ------------------------------
def parse_torque(value):

    if pd.isna(value):
        return np.nan

    value = str(value).strip().lower()

    num = re.findall(r'[\d\.]+', value)

    if not num:
        return np.nan

    num = float(num[0])

    if "lb" in value:
        return num * 1.35582

    return num


def clean_torque(df):

    df["Torque"] = df["Torque"].apply(parse_torque)
    df["Torque"] = pd.to_numeric(df["Torque"], errors="coerce")

    median = df["Torque"].median()

    df["Torque"] = df["Torque"].fillna(median)

    return df


# ------------------------------
# 6. Temperature Conversion
# ------------------------------
def convert_temp(value):

    if pd.isna(value):
        return np.nan

    value = str(value).strip().upper()

    try:

        if "F" in value:
            num = float(value.replace("F", ""))
            return (num - 32) * 5 / 9

        elif "C" in value:
            return float(value.replace("C", ""))

        else:
            return float(value)

    except:
        return np.nan


def clean_temperature(df):

    df["Temp_C"] = df["Temp"].apply(convert_temp)

    df.drop(columns=["Temp"], inplace=True)

    median = df["Temp_C"].median()

    df["Temp_C"] = df["Temp_C"].fillna(median)

    return df


# ------------------------------
# 7. Pressure Conversion
# ------------------------------
def clean_pressure(df):

    df["Pressure"] = df["Pressure"].astype(str).str.strip().str.upper()

    pressure_val = df["Pressure"].str.extract(r'(\d+\.?\d*)')[0].astype(float)

    mask = df["Pressure"].str.contains("PSI", na=False)

    pressure_val.loc[mask] = pressure_val.loc[mask] / 14.5038

    df["Pressure"] = pressure_val

    return df


# ------------------------------
# 8. Defect Normalization
# ------------------------------
def normalize_defects(df):

    df["Defect"] = df["Defect"].astype(str).str.strip().str.upper()

    df["Defect"] = df["Defect"].replace({
        "REP": "REPAIR",
        "REPAIR": "REPAIR",
        "REJ": "REJECT",
        "REJECTED": "REJECT",
        "OK": "OK",
        "PASS": "OK",
        "NA": "UNKNOWN",
        "N/A": "UNKNOWN",
        "NONE": "UNKNOWN",
        "NULL": "UNKNOWN"
    })

    return df


# ------------------------------
# 9. VIN Validation
# ------------------------------
def validate_vin(df):

    df["VIN"] = df["VIN"].astype(str).str.strip().str.upper()

    pattern = r'^[A-HJ-NPR-Z0-9]{17}$'

    df = df[df["VIN"].str.match(pattern)]

    df["VIN"] = df["VIN"].str[:3] + "***********" + df["VIN"].str[-3:]

    return df


# ------------------------------
# 10. Remove Duplicate Events
# ------------------------------
def remove_duplicate_events(df):

    df = df.drop_duplicates(subset=["VIN", "Station", "TS"], keep="first")

    return df


# ------------------------------
# 11. Sensor Calibration
# ------------------------------
def apply_sensor_calibration(df):

    factor = df["Calibration_Factor"].astype(float)

    df["Torque"] = df["Torque"] * factor
    df["Temp_C"] = df["Temp_C"] * factor
    df["Pressure"] = df["Pressure"] * factor

    return df


# ------------------------------
# 12. Cycle Time Validation
# ------------------------------
def validate_cycle_time(df):

    df = df.sort_values(["Station", "TS"])

    cycle_time = df.groupby("Station")["TS"].diff().dt.total_seconds()

    implausible = (cycle_time <= 0) | (cycle_time > 1800)

    df = df.loc[~implausible]

    return df


# ------------------------------
# 13. Rework Detection
# ------------------------------
def detect_rework(df):

    visits = df.groupby(["VIN", "Station"]).size()

    rework = visits[visits > 1]

    rework_vins = rework.index.get_level_values("VIN").unique()

    df["Rework_Flag"] = df["VIN"].isin(rework_vins)

    return df


# ------------------------------
# 14. Shift Derivation
# ------------------------------
def derive_shift(df):

    hour = df["TS"].dt.hour

    df["Shift_Derived"] = np.select(
        [
            (hour >= 6) & (hour < 14),
            (hour >= 14) & (hour < 22)
        ],
        ["Morning", "Evening"],
        default="Night"
    )

    mismatch = df["Shift"] != df["Shift_Derived"]

    df.loc[mismatch, "Shift"] = df.loc[mismatch, "Shift_Derived"]

    return df


# ------------------------------
# 15. Save Output
# ------------------------------
def save_data(df, path):
    df.to_csv(path, index=False)


# ------------------------------
# MAIN PIPELINE
# ------------------------------
def main():

    df = load_data("C:\\Users\\sunit\\OneDrive\\Desktop\\AssgnAgarwal\\DataCleaning\\Case_Study\\manufacturing_data_pipeline\\data\\raw_manufacturing_data_final_distribution.csv")

    df = clean_line_station(df)

    df = clean_timestamp(df)

    df = normalize_part_numbers(df)

    df = clean_torque(df)

    df = clean_temperature(df)

    df = clean_pressure(df)

    df = normalize_defects(df)

    df = validate_vin(df)

    df = remove_duplicate_events(df)

    df = apply_sensor_calibration(df)

    df = validate_cycle_time(df)

    df = detect_rework(df)

    df = derive_shift(df)

    save_data(df, "cleaned_manufacturing_data.csv")

    print("Data cleaning pipeline completed successfully.")


# ------------------------------
# Run Script
# ------------------------------
if __name__ == "__main__":
    main()