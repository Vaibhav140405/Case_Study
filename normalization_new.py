import pandas as pd
import numpy as np

df = pd.read_csv('raw_data.csv')

df = df.drop_duplicates()

# ── Use Case 1: Line/Station code trimming and canonical case ──
df['Line'] = df['Line'].astype(str).str.strip().str.upper()
df["Station"] = df["Station"].astype(str).str.strip().str.upper()

df['Line'] = df['Line'].str.replace(" ", "", regex=False)
df['Station'] = df['Station'].str.replace(" ", "", regex=False)

# ── Use Case 2: Timestamp normalization; invalid minutes fix ──
df["TS"] = df["TS"].ffill()

df["TS"] = df["TS"].str.replace(" :", ":", regex=False)

time = df["TS"].str.split(" ").str[1]

hour = time.str.split(":").str[0].astype(int)
minute = time.str.split(":").str[1].astype(int)

hour = hour + (minute//60)
minute = minute % 60

df["TS"] = df["TS"].str.split(" ").str[0] + " " + hour.astype(str).str.zfill(2) + ":" + minute.astype(str).str.zfill(2)

df["TS"] = pd.to_datetime(df["TS"], errors="coerce", format="mixed",dayfirst=True)

shift_map = {
    "Morning": "06:00:00",
    "Evening": "14:00:00",
    "Night": "22:00:00"
}

mask = df["TS"].isna()

df.loc[mask, "TS"] = pd.to_datetime(
    "2026-03-10 " + df.loc[mask, "Shift"].map(shift_map)
)

# ── Use Case 3: Part number normalization (upper, hyphen rules) and BOM validation ──

df["Part_No"] = df["Part_No"].astype(str).str.strip().str.upper()

# fix missing hyphen
df["Part_No"] = df["Part_No"].str.replace(r'^(\d{3})([A-Z]{3})$', r'\1-\2', regex=True)

# validate format   
df["Part_Format_Valid"] = df["Part_No"].str.match(r'^\d{3}-[A-Z]{3}$')

bom_parts = df["Part_No"].unique() 
df["Part_In_BOM"] = df["Part_No"].isin(bom_parts)

# ── Use Case 4: Torque numeric parsing to Nm, unit checks ──
# clean torque column
df["Torque"] = df["Torque"].astype(str).str.strip().str.upper()

# extract numeric value
torque_val = df["Torque"].str.extract(r'(\d+\.?\d*)')[0].astype(float)

# detect lb-ft values
mask = df["Torque"].str.contains("LB")

# convert lb-ft to Nm
torque_val.loc[mask] = torque_val.loc[mask] * 1.35582

# replace original column with cleaned Nm values
df["Torque"] = torque_val

# ── Use Case 5: Temperature unit parsing (C/F) to Celsius ──
# clean temperature column
df["Temp"] = df["Temp"].astype(str).str.strip().str.upper()

# extract numeric value
temp_val = df["Temp"].str.extract(r'(\d+\.?\d*)')[0].astype(float)

# extract unit
temp_unit = df["Temp"].str.extract(r'([CF])')[0]

# convert to Celsius
df["Temp"] = temp_val
df.loc[temp_unit == "F", "Temp_C"] = (temp_val - 32) * 5/9

# ── Use Case 6: Defect field normalization (None/OK/Reject/Repair) ──
df["Defect"] = df["Defect"].astype(str).str.strip().str.upper()

df["Defect"] = df["Defect"].replace({
    "REP": "Repair",
    "REPAIR": "Repair",
    "REJ": "Reject",
    "REJECT": "Reject",
    "NAN": "None",
    "NONE":"None"
})

# ── Use Case 7: VIN format validation and masking for PII when exporting ──
# clean VIN
df["VIN"] = df["VIN"].astype(str).str.strip().str.upper()

# keep only valid VINs (17 characters, no I O Q)
df = df[df["VIN"].str.match(r'^[A-HJ-NPR-Z0-9]{17}$')]

# mask VIN for export
df["VIN"] = df["VIN"].str[:3] + "***********" + df["VIN"].str[-3:]

# ── Use Case 8: Supplier code normalization and master join ──
# normalize supplier code
df["Supplier"] = df["Supplier"].astype(str).str.strip().str.upper()

# extract numeric part
sup_num = df["Supplier"].str.extract(r'(\d+)')[0].astype(float)

# create standard code
df["Supplier"] = "SUP-" + sup_num.astype("Int64").astype(str).str.zfill(2)

# ── Use Case 9: Duplicate event dedup by (VIN, Station, TS) ──
# identify duplicates
df["Is_Duplicate"] = df.duplicated(subset=["VIN","Station","TS"], keep="first")

# count duplicates
df["Is_Duplicate"].sum()

#remove duplicates
df = df[~df["Is_Duplicate"]]

# ── Use Case 10: Sensor calibration factors and drift checks ──
#Clean pressure column
df["Pressure"] = df["Pressure"].astype(str).str.strip().str.upper()

#Extract numeric value
pressure_val = df["Pressure"].str.extract(r'(\d+\.?\d*)')[0].astype(float)

#Detect PSI values
mask = df["Pressure"].str.contains("PSI", na=False)

#Convert psi to bar
pressure_val.loc[mask] = pressure_val.loc[mask] / 14.5038

#Replace original column
df["Pressure"] = pressure_val

#Apply calibration factor
factor = df["Calibration_Factor"].astype(float)

df["Torque"] = df["Torque"] * factor
df["Temp"] = df["Temp"] * factor
df["Pressure"] = df["Pressure"] * factor

#Detect torque drift
df = df.sort_values(["Machine_ID","TS"])

rolling_mean = df.groupby("Machine_ID")["Torque"].rolling(50).mean().reset_index(level=0, drop=True)

drift = rolling_mean.diff().abs() > 5

# ── Use Case 11: Cycle time plausibility (TS deltas) per station ──
#Sort events
df = df.sort_values(["Station","TS"])
#Compute time difference
cycle_time = df.groupby("Station")["TS"].diff().dt.total_seconds()

#Detect Implausible Times
implausible = (cycle_time <= 0) | (cycle_time > 1800)

#removing 
df = df.loc[~implausible]
df = df.reset_index(drop=True)

# ── Use Case 12: Work order linkage validation (WO→VIN) ──
wo_counts = df.groupby("VIN")["WO"].nunique()
wo_counts[wo_counts > 1]

# ── Use Case 13: Rework loop identification and tagging ──
#Detect repeated station visits
station_visits = df.groupby(["VIN","Station"]).size()
#Identify rework cases
rework = station_visits[station_visits > 1]
#Extract VINs with rework
rework_vins = rework.index.get_level_values("VIN").unique()

#Tagging rework
df["Rework_Flag"] = df["VIN"].isin(rework_vins)

# ── Use Case 14: Shift code derivation and validation ──
hour = df["TS"].dt.hour
df["Shift_Derived"] = np.select(
    [
        (hour >= 6) & (hour < 14),
        (hour >= 14) & (hour < 22)
    ],
    ["Morning","Evening"],
    default="Night"
)

shift_mismatch = df["Shift"] != df["Shift_Derived"]
shift_mismatch.sum()

df.loc[shift_mismatch, "Shift"] = df.loc[shift_mismatch, "Shift_Derived"]

# ── Use Case 15: Scrap reason code mapping ──
# Replace missing scrap reasons with a placeholder
df['Scrap_Reason'] = df['Scrap_Reason'].fillna("Unknown")

# Standardize formatting
df['Scrap_Reason'] = (
    df['Scrap_Reason']
    .astype(str)
    .str.strip()
    .str.title()
)

# ── Use Case 16: Unit conversions for torque/temperature/pressure ──
# Remove text units and convert to numeric

df['Torque_Nm'] = (
    df['Torque']
    .astype(str)
    .str.replace("Nm", "", regex=False)
    .str.replace("nm", "", regex=False)
    .str.strip()
)

df['Torque_Nm'] = pd.to_numeric(df['Torque_Nm'], errors='coerce')

def convert_temp(value):
    value = str(value).strip()
    
    try:
        if "F" in value:
            f = float(value.replace("F",""))
            return (f - 32) * 5/9
        elif "C" in value:
            return float(value.replace("C",""))
        else:
            return float(value)
    except:
        return None


df['Temp_C'] = df['Temp'].apply(convert_temp)

def convert_pressure(value):
    value = str(value).lower().strip()

    try:
        if "psi" in value:
            psi = float(value.replace("psi",""))
            return psi * 0.0689476
        elif "bar" in value:
            return float(value.replace("bar",""))
        else:
            return float(value)
    except:
        return None


df['Pressure_Bar'] = df['Pressure'].apply(convert_pressure)

# ── Use Case 17: Anomaly detection for out-of-spec readings ──
# Define acceptable manufacturing ranges

TORQUE_MIN = 20
TORQUE_MAX = 80

TEMP_MIN = 10
TEMP_MAX = 120

PRESSURE_MIN = 1
PRESSURE_MAX = 15

CYCLE_MIN = 5
CYCLE_MAX = 300

# Convert Torque column to numeric Nm

df['Torque_Nm'] = (
    df['Torque']
    .astype(str)
    .str.replace('Nm', '', regex=False)
    .str.replace('nm', '', regex=False)
    .str.strip()
)

df['Torque_Nm'] = pd.to_numeric(df['Torque_Nm'], errors='coerce')

def convert_temp(value):
    value = str(value).strip()

    try:
        if "F" in value:
            f = float(value.replace("F",""))
            return (f - 32) * 5/9
        elif "C" in value:
            return float(value.replace("C",""))
        else:
            return float(value)
    except:
        return None


df['Temp_C'] = df['Temp'].apply(convert_temp)

def convert_pressure(value):
    value = str(value).lower().strip()

    try:
        if "psi" in value:
            psi = float(value.replace("psi",""))
            return psi * 0.0689476
        elif "bar" in value:
            return float(value.replace("bar",""))
        else:
            return float(value)
    except:
        return None


df['Pressure_Bar'] = df['Pressure'].apply(convert_pressure)

# Torque anomaly detection

df['Torque_Anomaly'] = (
    (df['Torque_Nm'] < TORQUE_MIN) |
    (df['Torque_Nm'] > TORQUE_MAX)
)

df['Temp_Anomaly'] = (
    (df['Temp_C'] < TEMP_MIN) |
    (df['Temp_C'] > TEMP_MAX)
)

df['Pressure_Anomaly'] = (
    (df['Pressure_Bar'] < PRESSURE_MIN) |
    (df['Pressure_Bar'] > PRESSURE_MAX)
)

df['Cycle_Time'] = pd.to_numeric(df['Cycle_Time'], errors='coerce')

df['Cycle_Anomaly'] = (
    (df['Cycle_Time'] < CYCLE_MIN) |
    (df['Cycle_Time'] > CYCLE_MAX)
)

df['Sensor_Anomaly'] = (
    df['Torque_Anomaly'] |
    df['Temp_Anomaly'] |
    df['Pressure_Anomaly'] |
    df['Cycle_Anomaly']
)

# ── Use Case 18: Human vs automated inspection source tagging ──
# Standardize inspection source values

df['Inspection_Source_Clean'] = (
    df['Inspection_Source']
    .astype(str)
    .str.strip()
    .str.lower()
)

inspection_map = {
    "human": "Human",
    "manual": "Human",
    
    "auto": "Automated",
    "automated": "Automated",
    "machine": "Automated"
}

df['Inspection_Source_Clean'] = df['Inspection_Source_Clean'].map(inspection_map)

df['Inspection_Source_Clean'] = df['Inspection_Source_Clean'].fillna("Unknown")

# ── Use Case 19: Tool ID mapping for torque tools ──
df['Tool_ID_Clean'] = (
    df['Tool_ID']
    .astype(str)
    .str.strip()
    .str.upper()
)

df['Tool_Number'] = df['Tool_ID'].str.extract(r'(\d+)').astype(int)

df['Tool_ID_Valid'] = df['Tool_ID_Clean'].str.match(r"^TL-\d{3}$")

tool_master = pd.DataFrame({
    "Tool_ID_Clean": [f"TL-{i:03d}" for i in range(1,31)],
    "Tool_Type": "Torque Wrench",
    "Calibration_Status": "Valid"
})

df = df.merge(tool_master, on="Tool_ID_Clean", how="left")

df['Tool_Master_Match'] = df['Tool_Type'].notna()

# ── Use Case 20: BOM version effectivity date checks ──
bom_master = pd.DataFrame({
    "BOM_Version": ["BOM-1", "BOM-2", "BOM-3"],
    "Effective_Date": [
        "2025-01-01",
        "2026-01-01",
        "2026-06-01"
    ]
})

bom_master["Effective_Date"] = pd.to_datetime(bom_master["Effective_Date"])

df = df.merge(bom_master, on="BOM_Version", how="left")

df['TS'] = pd.to_datetime(df['TS'], errors='coerce')

df['BOM_Valid'] = df['TS'] >= df['Effective_Date']

# ── Column Cleanup ──
# 1. Intermediate / validation helper columns
intermediate_cols = [
    'Part_Format_Valid', 'Part_In_BOM', 'Is_Duplicate',
    'Shift_Derived', 'Tool_ID_Clean', 'Tool_Number',
    'Tool_ID_Valid', 'Tool_Type', 'Calibration_Status',
    'Tool_Master_Match', 'BOM_Valid', 'Effective_Date'
]

# 2. Redundant derived columns (originals already contain cleaned values)
redundant_cols = ['Torque_Nm', 'Temp_C', 'Pressure_Bar']

# 3. Individual anomaly flags (rolled into Sensor_Anomaly)
anomaly_detail_cols = [
    'Torque_Anomaly', 'Temp_Anomaly',
    'Pressure_Anomaly', 'Cycle_Anomaly'
]

# 4. Original columns never referenced in any use case
unused_original_cols = [
    'Vibration', 'Humidity', 'Voltage', 'Current',
    'Energy_Consumption', 'Supplier_Lot', 'Batch_ID',
    'Production_Order', 'Material_Type', 'Assembly_Step',
    'Sensor_Status', 'Quality_Score', 'Andon_Flag',
    'Production_Target', 'Actual_Output',
    'Maintenance_Flag', 'Warranty_Flag',
    'Calibration_Factor'
]

all_drop = intermediate_cols + redundant_cols + anomaly_detail_cols + unused_original_cols
all_drop = [c for c in all_drop if c in df.columns]

df = df.drop(columns=all_drop)

# 5. Replace Inspection_Source with the cleaned version
if 'Inspection_Source' in df.columns and 'Inspection_Source_Clean' in df.columns:
    df = df.drop(columns=['Inspection_Source'])
df = df.rename(columns={'Inspection_Source_Clean': 'Inspection_Source'})

print(f'Final column count: {len(df.columns)}')
print(f'Final columns:\n{list(df.columns)}')
