# Databricks notebook source
# Schema is stored in set-up/Schema"
from pyspark.sql.functions import col
Taxi_Zone_Df = spark.read.format("csv") \
                     .option("InferSchema" , True)\
                     .option("Header" , True)\
                     .load("/mnt/covidreportingdatalake6/streamread").show()

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

Taxi_Zone_Df.write.format("parquet") \
                  .partitionBy("Service_Zone") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable("newyork_taxi.taxi_zone_int")
