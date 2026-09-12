# ASBD CSV Tools Streamlit App

This Streamlit app provides two upload-driven tools: 12b-1 representative commission allocation and 529 account filtering.

## Requirements

Install runtime dependencies:

```powershell
pip install -r requirements.txt
```

Requires Python 3.8+ (or compatible) and the packages listed in `requirements.txt`.

## CSV format

Upload a CSV with at least the following columns:

```
RepNumber,CommissionAmount,TrailerPayoutDate
12345,250.00,20260102
```

Commission values should be numeric, and `TrailerPayoutDate` should use the `YYYYMMdd` format. The app validates these values and shows the data date beneath the total row.

## Run

```powershell
streamlit run main.py
```

Open the URL shown in the terminal to view the app.

## Usage

- Choose **12B-1 Report** or **529 Calculator** from the toolbar at the top of the page.
- Upload one or more CSV files and process them in the selected tool.
- The 12B-1 report uses an in-memory DuckDB connection and the `idc_dst_12b1_rep_allocation.report` module to:
  - load the uploaded sales data
  - validate the data (shows validation errors if any)
  - compute aggregated commissions by `RepNumber` and display them as a table
- The 529 Calculator filters rows where `SocialCode` is `529`, keeps the calculator columns from `529Calculator.py`, adds numeric totals, and provides a CSV download.
- Uploaded files are processed in memory; the app does not require a server-side input folder.

## Developer notes

- The main entrypoint is `main.py` which calls functions from `idc_dst_12b1_rep_allocation/report.py`:
  - `load_sales_data(conn, uploaded_file)` — loads CSV into DuckDB
  - `validate_sales_data(conn)` — returns a list of validation error messages
  - `get_rep_commissions(conn, include_zeros)` — returns a DataFrame of aggregated commissions
- The app opens a temporary DuckDB connection for each run and closes it afterwards.
