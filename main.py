
import pandas as pd
import sqlite3
from datetime import datetime

print("Starting MetroFlow ETL Pipeline...")

# Extract
df = pd.read_csv("data/raw_data.csv")
print("Data extracted successfully.")

# Transform
print("Applying transformations...")
df['rent'] = df['rent'].fillna(0)
df = df.drop_duplicates()
df['processed_at'] = datetime.now()

print("Transformations completed:")
print("- Missing values handled")
print("- Duplicates removed")
print("- Timestamp column added")

# Load
print("Loading data into SQLite warehouse...")
conn = sqlite3.connect("warehouse.db")
df.to_sql("tenants", conn, if_exists="replace", index=False)
conn.close()

print("Data loaded successfully into warehouse.db")
print("MetroFlow ETL Pipeline completed successfully.")
