# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

#aggregate df by day

df = df.\
    groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
        agg(
            count("*").alias("total_trips"),
            round(avg("passenger_count"),1).alias("average_passengers"),
            round(avg("trip_distance"),1).alias("average_distance"),
            round(avg("fare_amount"),2).alias("average_fare_per_trip"),
            max("fare_amount").alias("max_fare"),
            min("fare_amount").alias("min_fare"),
            round(sum("fare_amount"),2).alias("total_revenue")
        )

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary")