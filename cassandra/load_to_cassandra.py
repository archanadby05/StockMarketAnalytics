import glob

import pandas as pd

from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

# Create keyspace and table
session.execute(
    """
CREATE KEYSPACE IF NOT EXISTS stock_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
"""
)

session.execute(
    """
CREATE TABLE IF NOT EXISTS stock_data.monthly_summary (
    symbol TEXT,
    year INT,
    month INT,
    avg_close DOUBLE,
    avg_monthly_return DOUBLE,
    PRIMARY KEY ((symbol), year, month)
);
"""
)

# Load the latest output CSV
csv_file = glob.glob("output/monthly_summary/part-*.csv")[0]
df = pd.read_csv(csv_file)

# Insert rows
for _, row in df.iterrows():
    session.execute(
        """
        INSERT INTO stock_data.monthly_summary (symbol, year, month, avg_close, avg_monthly_return)
        VALUES (%s, %s, %s, %s, %s)
    """,
        (
            row["Symbol"],
            int(row["year"]),
            int(row["month"]),
            float(row["avg_close"]),
            float(row["avg_monthly_return"]),
        ),
    )

print("Data successfully inserted into Cassandra.")

cluster.shutdown()
