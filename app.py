import pandas as pd
import streamlit as st

from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(["127.0.0.1"])
session = cluster.connect("stock_data")

st.title("Stock Analytics Dashboard")

# Sidebar for symbol selection
symbols = session.execute("SELECT DISTINCT symbol FROM monthly_summary")
symbol_list = [row.symbol for row in symbols]
selected_symbol = st.sidebar.selectbox("Select Symbol", symbol_list)

# Fetch data for selected symbol
query = f"""
SELECT year, month, avg_close, avg_monthly_return
FROM monthly_summary
WHERE symbol = '{selected_symbol}'
ORDER BY year, month;
"""
rows = session.execute(query)

# Convert to DataFrame
data = pd.DataFrame(
    [
        {
            "year": r.year,
            "month": r.month,
            "avg_close": r.avg_close,
            "avg_monthly_return": r.avg_monthly_return,
        }
        for r in rows
    ]
)

st.subheader(f"Monthly Summary for {selected_symbol}")
st.dataframe(data)

if not data.empty:
    data["year_month"] = (
        data["year"].astype(str) + "-" + data["month"].astype(str).str.zfill(2)
    )
    data = data.set_index("year_month")
    st.line_chart(data[["avg_close", "avg_monthly_return"]])

cluster.shutdown()
