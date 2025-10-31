from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, year, month
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("StockProcessing").getOrCreate()

df = spark.read.option("header", "true").csv("../data/nifty50_long.csv")
df = df.withColumn("Close", col("Close").cast("double"))
df = df.withColumn("year", year(col("DATE")))
df = df.withColumn("month", month(col("DATE")))

windowSpec = Window.partitionBy("Symbol").orderBy("DATE")
df = df.withColumn("prev_close", lag("Close").over(windowSpec))
df = df.withColumn("daily_return", (col("Close") - col("prev_close")) / col("prev_close"))
df = df.na.drop(subset=["daily_return"])
df = df.select("Symbol", "DATE", "year", "month", "Close", "daily_return")

df.coalesce(1).write.mode("overwrite").option("header", "true").csv("../output/daily_summary")

spark.stop()
