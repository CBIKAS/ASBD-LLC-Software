import duckdb

# Create a DuckDB connection
conn = duckdb.connect()

# Read the CSV file into a table named 'sales_data'
conn.execute("CREATE TABLE sales_data AS SELECT * FROM 'DTCCRC8.A0597.csv'")

# Group the data by product_category and calculate the total sales
query = """
SELECT RepNumber, Round(SUM(CommissionAmount),2) AS total_sales
FROM sales_data
GROUP BY RepNumber
"""

# Execute the query and fetch the results
results = conn.execute(query).fetchall()

# Print the results
for row in results:
    print(f"RepNumber: {row[0]}, Total Sales: {row[1]}")

# Group the data by product_category and calculate the total sales
query = """
SELECT Round(SUM(CommissionAmount), 2) AS total_sales
FROM sales_data
"""

# Execute the query and fetch the results
results = conn.execute(query).fetchall()

# Print the results
for row in results:
    print(f"Total Sales: {row[0]}")

# Close the connection
conn.close()