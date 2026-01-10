# 12B1 Rep Allocation Streamlit App

This Streamlit app aggregates commission amounts by representative (RepNumber) for 12b-1 rep allocation reporting. It validates uploaded sales data and shows aggregated commissions per rep.

## Requirements

Install runtime dependencies:

```powershell
pip install -r requirements.txt
```

Requires Python 3.8+ (or compatible) and the packages listed in `requirements.txt`.

## CSV format

Upload a CSV with at least the following columns:

```
RepNumber,CommissionAmount
12345,250.00
```

Commission values should be numeric. The app will validate rows and display errors if present.

## Run

```powershell
streamlit run main.py
```

Open the URL shown in the terminal to view the app.

## Usage

- In the app choose a CSV file to upload.
- Click **Submit**. If no file is uploaded, the app shows an error.
- The app uses an in-memory DuckDB connection and the `idc_dst_12b1_rep_allocation.report` module to:
  - load the uploaded sales data
  - validate the data (shows validation errors if any)
  - compute aggregated commissions by `RepNumber` and display them as a table

## Developer notes

- The main entrypoint is `main.py` which calls functions from `idc_dst_12b1_rep_allocation/report.py`:
  - `load_sales_data(conn, uploaded_file)` — loads CSV into DuckDB
  - `validate_sales_data(conn)` — returns a list of validation error messages
  - `get_rep_commissions(conn, include_zeros)` — returns a DataFrame of aggregated commissions
- The app opens a temporary DuckDB connection for each run and closes it afterwards.
