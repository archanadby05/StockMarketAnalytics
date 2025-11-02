# Stock Market Analytics Pipeline

### Overview

An end-to-end analytics pipeline for **NIFTY-50 stock data**, integrating **Apache Spark**, **Apache Cassandra**, and **Streamlit**. The system cleans raw stock data, computes daily and monthly summaries, stores them in Cassandra, and visualizes insights through an interactive dashboard.

---

### Architecture

1. **Preprocessing** – `preprocessing_data.ipynb` cleans and reshapes `nifty50.csv` into `nifty50_long.csv`.
2. **Processing (Spark)** – `process_data.py` computes daily metrics; `monthly_summary.py` aggregates them monthly.
3. **Storage (Cassandra)** – `load_to_cassandra.py` and `load_data.py` define schema and insert processed summaries.
4. **Visualization (Streamlit)** – `app.py` displays trends, summaries, and charts from Cassandra data.

---

### Tech Stack

| Layer         | Technology       |
| ------------- | ---------------- |
| Processing    | Apache Spark     |
| Storage       | Apache Cassandra |
| Visualization | Streamlit        |
| Language      | Python           |
| Dataset       | NIFTY-50 (CSV)   |

---

### Repository Structure

```
cassandra/
  ├── load_data.py
  └── load_to_cassandra.py
data/
  ├── nifty50.csv
  ├── nifty50_long.csv
  └── preprocessing_data.ipynb
spark_jobs/
  ├── process_data.py
  └── monthly_summary.py
app.py
README.md
.gitignore
```

---

### Usage

```bash
# Run Spark processing
python spark_jobs/process_data.py
python spark_jobs/monthly_summary.py

# Load data into Cassandra
python cassandra/load_to_cassandra.py

# Launch dashboard
streamlit run app.py
```

---

### Features

* Automated Spark-based summary generation.
* Distributed storage with Cassandra.
* Interactive analytics via Streamlit dashboard.
* Modular and extensible design.

---

### Author

**Archana Dubey**
[GitHub: @archanadby05](https://github.com/archanadby05)
