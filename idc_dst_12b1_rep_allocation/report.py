import duckdb
import pandas as pd
from typing import List, Dict, Union, IO


def load_sales_data(conn: duckdb.DuckDBPyConnection, csv_source: Union[str, IO]) -> None:
    """Load CSV data into the given DuckDB connection as table `sales_data`.

    csv_source may be a filesystem path (str) or a file-like object (BytesIO/TextIO).
    The function will register a temporary dataframe and create `sales_data` from it.
    """
    if isinstance(csv_source, str):
        # read directly from path using DuckDB's read_csv_auto
        conn.execute(f"""
            CREATE OR REPLACE TABLE sales_data AS
            SELECT * FROM read_csv_auto('{csv_source}', header=True, delim=',', quote='"', strict_mode=False)
        """)
    else:
        # assume file-like object: read with pandas and register
        df = pd.read_csv(csv_source)
        conn.register('__upload_df', df)
        conn.execute("CREATE OR REPLACE TABLE sales_data AS SELECT * FROM __upload_df")


def validate_sales_data(conn: duckdb.DuckDBPyConnection, minimum_rows_expected: int = 2) -> List[str]:
    """Validate that `sales_data` contains required columns and sensible values.

    Returns a list of error messages. Empty list means validation passed.
    """
    errors: List[str] = []

    # Check table exists
    try:
        table_info = conn.execute("PRAGMA table_info('sales_data')").fetchall()
    except Exception:
        errors.append("Table 'sales_data' does not exist in the provided connection.")
        return errors

    column_names = [col[1] for col in table_info]
    required = {"RepNumber", "CommissionAmount"}
    missing = required - set(column_names)
    if missing:
        errors.append(f"Missing required columns: {', '.join(sorted(missing))}")

    # Count null/blank RepNumber
    res = conn.execute("SELECT COUNT(*) FROM sales_data WHERE RepNumber IS NULL OR RepNumber = ''").fetchone()
    null_rep_count = int(res[0]) if res else 0
    if null_rep_count > 0:
        errors.append(f"Null or blank RepNumber count: {null_rep_count}")

    # Total rows
    res = conn.execute("SELECT COUNT(*) FROM sales_data").fetchone()
    total_rows = int(res[0]) if res else 0
    if total_rows < minimum_rows_expected:
        errors.append(f"Total rows ({total_rows}) less than minimum expected ({minimum_rows_expected})")

    # CommissionAmount numeric check
    res = conn.execute("SELECT COUNT(*) FROM sales_data WHERE TRY_CAST(CommissionAmount AS DOUBLE) IS NULL").fetchone()
    non_numeric = int(res[0]) if res else 0
    if non_numeric > 0:
        errors.append(f"Non-numeric CommissionAmount count: {non_numeric}")

    return errors


def get_rep_commissions(conn: duckdb.DuckDBPyConnection, include_total: bool = False) -> pd.DataFrame:
    """Return a pandas DataFrame with columns `RepNumber` and `CommissionAmount` (summed, rounded).
    If `include_total` is True, append a placeholder rep named 'Total' with the sum of all commissions.
    Requires `sales_data` to exist in the connection.
    """
    query = """
    SELECT
        Replace(Replace(RepNumber, '"', ''), '=', '') AS RepNumber,
        ROUND(SUM(CAST(CommissionAmount AS DOUBLE)), 2) AS CommissionAmount
    FROM sales_data
    GROUP BY RepNumber
    ORDER BY RepNumber
    """
    df = conn.execute(query).df()

    if include_total:
        # Ensure CommissionAmount is numeric, sum and round to 2 decimals
        total = float(df['CommissionAmount'].sum()) if not df.empty else 0.0
        total = round(total, 2)
        # Append Total row at the end
        total_row = pd.DataFrame([{'RepNumber': 'Total', 'CommissionAmount': total}])
        df = pd.concat([df, total_row], ignore_index=True)

    return df
