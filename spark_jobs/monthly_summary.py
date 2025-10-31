from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Initialize Spark session
spark = SparkSession.builder.appName("MonthlyStockSummary").getOrCreate()

# Read processed daily summary
df = spark.read.option("header", True).csv("output/daily_summary/part-*.csv")

# Convert numeric columns
df = df.withColumn("Close", col("Close").cast("double"))
df = df.withColumn("daily_return", col("daily_return").cast("double"))

# Group by symbol, year, month to get monthly averages
monthly_df = (
    df.groupBy("Symbol", "year", "month")
      .agg(
          avg("Close").alias("avg_close"),
          avg("daily_return").alias("avg_monthly_return")
      )
      .orderBy("Symbol", "year", "month")
)

# Save output
monthly_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("output/monthly_summary")

spark.stop()
