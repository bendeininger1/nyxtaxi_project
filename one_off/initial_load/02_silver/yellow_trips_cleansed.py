# Databricks notebook source
from pyspark.sql.functions import col, when, timestamp_diff

# COMMAND ----------

df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC #check to make sure there is 6 months of data as only 6 months of data was downloaded (Jan 2025 - June 2025)
# MAGIC from pyspark.sql.functions import min, max
# MAGIC
# MAGIC df.agg(max("tpep_pickup_datetime"), min("tpep_pickup_datetime")).display()

# COMMAND ----------

#filter data to only include data from April 2025 - October 2025

df = df.filter("tpep_pickup_datetime >= '2025-04-01' AND tpep_pickup_datetime <= '2025-11-01'")

# COMMAND ----------

#convert ID data into 'actual data' based on data dictonary below
#https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

df = df.select(
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
        .when(col("VendorID") == 2, "Curb Mobility, LLC")
        .when(col("VendorID") == 6, "Myle Technologies Inc")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Unknown")
        .alias("vendor"),

    col("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime"),
    timestamp_diff('MINUTE', df.tpep_pickup_datetime, df.tpep_dropoff_datetime).alias("trip_duration"),
    col("passenger_count"),
    "trip_distance",

    when(col("RatecodeID") == 1, "Standard rate")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau or Westchester")
        .when(col("RatecodeID") == 5, "Negotiated fare")
        .when(col("RatecodeID") == 6, "Group ride")
        .otherwise("Unknown")
        .alias("rate_type"),

    "store_and_fwd_flag",
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),
    
    when(col("payment_type") == 0, "Flex Fare trip")
        .when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Unkown")
        .alias("payment_type"),
    
    col("fare_amount"),
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "processed_timestamp"
)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.yellow_trips_cleansed")
