# Manufacturing Data Pipeline — How It Works

This document explains the complete flow of the pipeline, step by step.
No prior coding knowledge is needed to follow along.

---

## What Does This Pipeline Do?

This pipeline **simulates a car factory's data system**. It:

1. **Generates** fake (but realistic) factory sensor data — messy, just like real-world data
2. **Cleans** the data — fixes errors, removes duplicates, standardizes formats
3. **Calculates** key performance metrics — how well the factory is doing
4. **Creates charts** — visual summaries saved as image files
5. **Prints a report** — a detailed summary in the terminal

---

## Project Structure

```
manufacturing_data_pipeline/
├── main.py                  # Entry point — run this to start everything
├── data_generator.py        # Step 1: Creates messy raw data
├── pipeline/
│   ├── config.py            # File name settings
│   └── runner.py            # Orchestrates all 5 steps in order
├── cleaning/                # Step 2: All data cleaning modules
│   ├── deduplication.py     # Remove duplicate rows
│   ├── normalization.py     # Fix text formats (uppercase, timestamps, etc.)
│   ├── unit_conversion.py   # Convert sensor units (F→C, PSI→bar, etc.)
│   ├── validation.py        # Validate VINs, flag anomalies
│   ├── missing_values.py    # Fill in blank fields
│   └── outlier_detection.py # Detect drifting sensors
├── transformations/         # Step 3: Compute KPIs and summary tables
│   ├── kpi_metrics.py       # Single-number metrics (OEE, FPY, etc.)
│   └── aggregations.py      # Grouped summaries (defects by type, etc.)
├── analysis/                # Step 4: Generate charts
│   └── charts.py            # 7 different chart types
├── utils/
│   └── helpers.py           # Shared utilities (file paths, CSV read/write)
├── data/                    # Raw and cleaned CSV files (auto-created)
└── output/
    ├── charts/              # PNG chart images (auto-created)
    └── reports/             # CSV summary tables (auto-created)
```

---

## Step-by-Step Flow

### Step 1 — Generate Raw Data (`data_generator.py`)

**What it does:** Creates ~7,000 rows of fake factory event data.

**Why it's messy on purpose:**
Real factory data comes from many different systems (sensors, scanners, manual entry)
and is never perfectly formatted. This generator mimics that by introducing:

- **Mixed text formats** — e.g. "GA-1", "ga-1", " Ga-1 " all mean the same production line
- **Mixed units** — temperatures in both Celsius and Fahrenheit, pressure in bar and PSI
- **Invalid timestamps** — e.g. "06:70" (70 minutes doesn't exist)
- **Bad VINs** — some vehicle IDs are too short or contain invalid characters
- **Duplicate rows** — the same event recorded more than once
- **Blank fields** — some values randomly left empty

Each row represents one **manufacturing event** (a part being worked on at a station).

**Key columns generated:** Event_ID, Line, Station, Machine_ID, Torque, Temperature,
Pressure, Cycle_Time, Defect, VIN, Supplier, and ~25 more.

**Output:** `data/raw_data.csv`

---

### Step 2 — Clean the Data (`cleaning/` package)

**What it does:** Fixes all the messiness from Step 1.  
**Input:** `data/raw_data.csv` → **Output:** `data/cleaned_data.csv`

The cleaning runs **23 steps** in a specific order:

#### Removing Bad Data
- **Remove exact duplicates** — rows that are 100% identical
- **Remove logical duplicates** — same vehicle + station + timestamp (recorded twice)
- **Validate VINs** — remove rows with invalid vehicle IDs (wrong length or bad characters)
- **Check cycle time plausibility** — remove events with impossible time gaps

#### Fixing Text Formats
- **Standardize Line & Station** — trim spaces, make uppercase ("ga-1" → "GA-1")
- **Fix timestamps** — parse different date formats, fix invalid minutes (06:70 → 07:10)
- **Normalize part numbers** — add missing hyphens, uppercase ("123abc" → "123-ABC")
- **Normalize defect labels** — map "REP", "repair", "Repair" all to "Repair"
- **Normalize supplier codes** — standardize to "SUP-01" format
- **Normalize inspection source** — map to "Human" or "Automated"
- **Clean tool IDs** — validate format, match against a master list
- **Fix scrap reasons** — fill blanks with "Unknown", apply title case

#### Converting Units
- **Torque** — convert lb-ft to Nm (multiply by 1.356)
- **Temperature** — convert Fahrenheit to Celsius ((F - 32) × 5/9)
- **Pressure** — convert PSI to bar (divide by 14.504)
- **Apply calibration factors** — multiply sensor readings by each sensor's correction factor

#### Detecting Issues
- **Detect sensor drift** — flag machines where the rolling torque average jumps suddenly
- **Flag anomalies** — mark readings outside safe ranges (e.g. torque outside 20–80 Nm)
- **Validate BOM versions** — check if the Bill of Materials was actually active on that date
- **Identify rework loops** — tag vehicles that visited the same station more than once
- **Validate work orders** — ensure each vehicle links to exactly one work order

#### Final Cleanup
- **Derive shift from timestamp** — Morning (6–14h), Evening (14–22h), Night (22–6h)
- **Handle missing values** — fill remaining blanks with "Unknown"
- **Mask VINs for privacy** — hide the middle characters ("ABC***********XYZ")
- **Drop temporary columns** — remove 26 intermediate columns no longer needed

**Result:** A clean dataset with ~4,000 rows and 45 columns, ready for analysis.

---

### Step 3 — Calculate Metrics (`transformations/` package)

**What it does:** Computes numbers and tables that summarize factory performance.

#### Single-Number KPIs (`kpi_metrics.py`)

- **OEE (Overall Equipment Effectiveness)** — Availability × Performance × Quality.
  A single number that shows how well the equipment is being used.
- **FPY (First Pass Yield)** — What % of units passed inspection without needing rework.
- **DPU (Defects Per Unit)** — What fraction of events had a Reject or Repair defect.
- **COPQ (Cost of Poor Quality)** — Dollar estimate of waste ($100 per reject, $30 per repair).
- **Andon Response Time** — How quickly the line reacts after someone pulls the Andon cord (stop signal).

#### Summary Tables (`aggregations.py`)

15 CSV reports are saved to `output/reports/`:

- **Defect Pareto** — Which defect types happen most (with cumulative %)
- **Cycle Time Stats** — Mean, standard deviation, quartiles
- **Station Bottlenecks** — Which stations are slowest
- **Supplier PPM** — Defective parts per million for each supplier
- **Shift Performance** — Output and first-pass yield by shift
- **Plan vs Actual** — Planned production vs actual output per line
- **PFMEA Risk Scores** — Risk Priority Number per defect code
- **Energy Per Vehicle** — Total energy used per vehicle
- **Rework Loops** — Vehicles that went through rework
- **Tool Calibration KPI** — How far each tool's torque deviates from the mean
- **Takt Time Adherence** — Are we meeting the production pace target?
- **Line Balance** — How evenly work is spread across stations
- **Throughput vs WIP** — Output compared to work-in-progress
- **Traceability Chain** — Supplier → Lot → Part → Vehicle mapping
- **Warranty Early Warning** — Part + defect combos that might cause warranty issues

---

### Step 4 — Generate Charts (`analysis/charts.py`)

**What it does:** Creates 7 charts saved as PNG images in `output/charts/`.

- **Defect Pareto** — Bar chart: which defects happen most
- **Cycle Time Histogram** — How cycle times are spread out
- **Supplier Defect Rate** — Bar chart: defect rate per supplier
- **Shift Productivity** — Bar chart: total output per shift
- **Torque SPC Chart** — Torque readings over time with control limits (±3σ)
- **Temperature SPC Chart** — Temperature readings over time with control limits
- **Station Bottleneck** — Bar chart: the 15 slowest stations

---

### Step 5 — Print Report (`pipeline/runner.py`)

**What it does:** Displays a detailed, formatted report in the terminal covering:

1. **Data Overview** — rows before/after cleaning, removal rate
2. **KPIs** — OEE, FPY, DPU, COPQ, Andon response time (with explanations)
3. **Defect Analysis** — breakdown by defect type with counts and percentages
4. **Station Performance** — average cycle time per station, slowest highlighted
5. **Shift Performance** — output, events, and first-pass yield per shift
6. **Supplier Quality** — defects per million by supplier
7. **Plan vs Actual** — planned vs actual output per line
8. **Anomaly Summary** — sensor anomalies, rework, maintenance, Andon, BOM validity
9. **Output Files** — list of generated files

---

## How to Run

```
python main.py
```

That's it. The pipeline runs all 5 steps automatically and prints the full report.

---

## Key Terms Glossary

- **OEE** — Overall Equipment Effectiveness. How efficiently equipment runs (0–100%).
- **FPY** — First Pass Yield. % of items that pass quality check on the first try.
- **DPU** — Defects Per Unit. Average number of defects per item produced.
- **COPQ** — Cost of Poor Quality. Estimated dollar cost of defects and rework.
- **Andon** — A signal (usually a cord or button) that stops the production line when a problem is found.
- **SPC** — Statistical Process Control. A method using charts to monitor if a process is stable.
- **Takt Time** — The pace at which products need to be made to meet demand.
- **BOM** — Bill of Materials. The list of parts and versions needed to build a product.
- **VIN** — Vehicle Identification Number. A unique 17-character ID for each vehicle.
- **PPM** — Parts Per Million. A way to express defect rates (lower is better).
- **PFMEA** — Process Failure Mode and Effects Analysis. A risk assessment method.
- **RPN** — Risk Priority Number. Severity × Occurrence × Detection (higher = more risky).
