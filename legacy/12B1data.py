import duckdb
from pathlib import Path
from typing import List, Tuple, Union

SalesResult = List[Tuple[str, float]]


def calculate_rep_sales(csv_path: Union[str, Path]) -> Tuple[SalesResult, float]:
    """Read the CSV file and return per-rep sales plus total sales."""
    csv_path = Path(csv_path)
    conn = duckdb.connect()

    conn.execute(
        f"""
        CREATE TABLE sales_data AS SELECT * FROM read_csv_auto('{csv_path}', HEADER=TRUE, DELIM=',', QUOTE='"', STRICT_MODE=FALSE)
        """
    )

    rep_query = """
    SELECT Replace(Replace(RepNumber, '"', ''), '=', '') AS RepNumber,
           Round(SUM(CAST(CommissionAmount AS DOUBLE)), 2) AS total_sales
    FROM sales_data
    GROUP BY RepNumber
    ORDER BY RepNumber
    """

    rep_sales = conn.execute(rep_query).fetchall()

    total_query = """
    SELECT Round(SUM(CAST(CommissionAmount AS DOUBLE)), 2) AS total_sales
    FROM sales_data
    """

    total_sales = conn.execute(total_query).fetchone()[0]
    conn.close()

    return rep_sales, total_sales


if __name__ == "__main__":
    import sys

    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('DTCCRC8.A0597.csv')
    per_rep_sales, total_sales = calculate_rep_sales(csv_path)

    for rep_number, rep_total in per_rep_sales:
        print(f"RepNumber: {rep_number}, Total Sales: {rep_total}")

    print(f"Total Sales: {total_sales}")
