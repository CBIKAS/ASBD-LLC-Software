import duckdb
from pathlib import Path

# Create a DuckDB connection
conn = duckdb.connect()

# Clean the raw CSV text so fields like ="001" become "001"
csv_path = Path('DTCCRC8.A0597.csv')

conn.execute(f"""
CREATE TABLE sales_data AS SELECT * FROM read_csv_auto('{csv_path}', HEADER=TRUE, DELIM=',', QUOTE='"', STRICT_MODE=FALSE)
""")

# Group the data by product_category and calculate the total sales
query = """
SELECT Replace(Replace(RepNumber, '"', ''), '=', '') AS RepNumber, Round(SUM(CAST(CommissionAmount AS DOUBLE)),2) AS total_sales
FROM sales_data
GROUP BY RepNumber
ORDER BY RepNumber
"""

# Execute the query and fetch the results
results = conn.execute(query).fetchall()

# Print the results
for row in results:
    print(f"RepNumber: {row[0]}, Total Sales: {row[1]}")

# Group the data by product_category and calculate the total sales
query = """
SELECT Round(SUM(CAST(CommissionAmount AS DOUBLE)), 2) AS total_sales
FROM sales_data
"""

# Execute the query and fetch the results
results = conn.execute(query).fetchall()

# Print the results
for row in results:
    print(f"Total Sales: {row[0]}")

# Close the connection
conn.close()