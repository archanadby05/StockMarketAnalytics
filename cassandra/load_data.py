import csv
from datetime import datetime

from cassandra.cluster import Cluster

cluster = Cluster(["127.0.0.1"])
session = cluster.connect("stockks")

insert_stmt = session.prepare(
    """
    INSERT INTO stock_summary (symbol, date, year, month, close, daily_return)
    VALUES (?, ?, ?, ?, ?, ?)
"""
)

with open("../output/daily_summary/part-00000.csv", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        session.execute(
            insert_stmt,
            (
                row["Symbol"],
                datetime.strptime(row["DATE"], "%Y-%m-%d").date(),
                int(row["year"]),
                int(row["month"]),
                float(row["Close"]),
                float(row["daily_return"]),
            ),
        )

cluster.shutdown()
print("Data loaded into Cassandra successfully.")
