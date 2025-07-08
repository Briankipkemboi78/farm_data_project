import duckdb

# Connect to the dbt-generated DuckDB file
con = duckdb.connect('dev.duckdb')

# Show all tables (optional)
print("📋 Available tables:")
print(con.execute("SHOW TABLES").fetchall())

# Query the 'entities' table
df = con.execute("SELECT * FROM entities_model LIMIT 10").fetchdf()

# Print the data
print("\n🔍 Preview of 'entities' table:")
print(df)

# Close the connection
con.close()