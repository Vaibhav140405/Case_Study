# Manufacturing Data Pipeline – Car Manufacturing

## 1. Project overview

This project simulates a **car manufacturing shop-floor data pipeline**. It:

- Generates **messy production event data** that mimics feeds from PLM/BOM, MES, IoT sensors, quality logs, supplier ASN, and warranty systems.
- Cleans and standardizes the data into an **analytics-ready dataset**.
- Calculates **manufacturing KPIs** such as OEE, FPY, DPU, supplier PPM, and energy per vehicle.
- Produces **visual charts** to support operations and quality discussions.

Everything is written in **Python + Pandas + NumPy + Matplotlib/Seaborn** with a clear, modular structure suitable for presentation.

## 2. Folder structure

Project root: `manufacturing_data_pipeline/`

- `data_generator.py` – Generate messy production events and save `data/raw_data.csv`.
- `data_cleaning.py` – Clean and normalize raw data and save `data/cleaned_data.csv`.
- `transformations.py` – Compute manufacturing KPIs and derived tables.
- `analysis.py` – Build charts from cleaned data and KPIs.
- `utils.py` – Shared helper functions (paths, mappings, parsing).
- `main.py` – Orchestrate the full pipeline end‑to‑end.
- `requirements.txt` – Python dependencies.
- `data/`
  - `raw_data.csv` – Generated messy dataset (created by the pipeline).
  - `cleaned_data.csv` – Cleaned dataset (created by the pipeline).
- `output/`
  - `charts/` – PNG charts for analysis (created by the pipeline).
  - `reports/` – Optional tabular/JSON exports (created by the pipeline).

## 3. How to run (Windows / PowerShell)

From the `manufacturing_data_pipeline` folder:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

python main.py
```

The pipeline will:

1. Generate a messy dataset (≈6000–7500 rows) and save `data/raw_data.csv`.
2. Clean and standardize the data and save `data/cleaned_data.csv`.
3. Compute KPIs and key manufacturing metrics.
4. Produce charts under `output/charts/`.
5. Print a short summary of key metrics to the console.

## 4. High‑level data dictionary

Key columns in the generated dataset (simplified descriptions):

- `Event_ID` – Unique event identifier on the shop floor.
- `Line` / `Station` – Production line and station codes.
- `Machine_ID`, `Tool_ID`, `Operator_ID` – Equipment and operator identifiers.
- `TS` – Event timestamp (messy formats in raw data).
- `Shift` / `Shift_Derived` – Planned and derived production shift.
- `Part_No`, `Part_Type`, `BOM_Version`, `Material_Type`, `Assembly_Step` – Product/assembly metadata.
- `Torque`, `Temp`, `Pressure`, `Vibration`, `Humidity`, `Voltage`, `Current`, `Energy_Consumption`, `Cycle_Time` – Sensor readings and timing.
- `Defect`, `Defect_Code`, `Scrap_Reason`, `Rework_Flag`, `Quality_Score`, `Inspection_Source`, `Inspection_Result` – Quality and inspection information.
- `VIN`, `Supplier`, `Supplier_Lot`, `Batch_ID`, `WO`, `Production_Order` – Traceability, supplier, and work order information.
- `Calibration_Factor`, `Sensor_Status`, `Andon_Flag`, `Production_Target`, `Actual_Output`, `Maintenance_Flag`, `Warranty_Flag` – Operational and maintenance context.

Cleaned data will also include:

- `TS_dt` – Normalized timestamp (Python datetime).
- `Torque_Nm`, `Temp_C`, `Pressure_bar` – Numeric, standardized units.
- `VIN_IsValid` – Flag for VIN format validity.
- `Anomaly_Flag`, `Anomaly_Reasons` – Simple anomaly indicators.

## 5. Customization guide

- **Row count** – Adjust number of simulated events in `data_generator.py`.
- **Lines/stations/machines** – Update reference lists in `data_generator.py` or shared configs in `utils.py`.
- **Cleaning rules** – Tune mappings and thresholds in `data_cleaning.py` (e.g., temperature/torque limits, acceptable ranges).
- **KPI definitions** – Update formulas or thresholds in `transformations.py`.
- **Charts** – Add or remove plots in `analysis.py` as needed for the presentation.

## 6. Example outputs (after implementation)

After running `python main.py`, you should have:

- `data/raw_data.csv` – Messy production events.
- `data/cleaned_data.csv` – Cleaned and standardized events.
- `output/charts/` – PNG charts such as:
  - `defect_pareto.png`
  - `cycle_time_histogram.png`
  - `supplier_defect_rate.png`
  - `shift_productivity.png`
  - `torque_spc.png`
  - `temperature_spc.png`
  - `station_bottleneck.png`

These files are suitable to showcase **data engineering practices** and to discuss **manufacturing performance and data quality** with stakeholders.

### Example cleaned dataset preview

Below is an example of the first few cleaned rows (columns truncated for readability):

| Event_ID | Line  | Station | TS_dt              | Part_No | Torque_Nm | Temp_C | Pressure_bar | Defect_Normalized | Shift_Derived |
|----------|-------|---------|--------------------|---------|-----------|--------|--------------|-------------------|---------------|
| 9        | GA-1  | STN-10  | 2026-10-03 06:04   | 311-GHI | 53.30     | 69.69  | 10.31        | Repair            | Morning       |
| 18       | GA-1  | STN-07  | 2026-10-03 06:08   | 993-JKL | 49.57     | 80.00  | 10.48        | None              | Morning       |
| 26       | PAINT-1 | STN-10 | 2026-10-03 06:12   | 982-JKL | 37.73     | 74.14  | 9.86         | Reject            | Morning       |
| 62       | PAINT-1 | STN-01 | 2026-10-03 06:30   | 710-DEF | 42.56     | 71.42  | 9.32         | Repair            | Morning       |
| 83       | GA-1  | STN-08  | 2026-10-03 06:41   | 693-GHI | 54.43     | 74.40  | 8.54         | Reject            | Morning       |


