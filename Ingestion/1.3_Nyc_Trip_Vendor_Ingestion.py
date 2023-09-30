# Databricks notebook source
# MAGIC %md 
# MAGIC #*INGEST VENDOR FILE*

# COMMAND ----------

dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3 - Reading Vendor  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC 1. Header is true as the file has header

# COMMAND ----------

Vendor_Df = spark.read.format("csv") \
                     .schema(Vendor_Schema)\
                     .option("Header" , True) \
                     .load(f"{DB_RAW}/raw/vendor_escaped.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 2 - Writing  Taxi Zone Parquet File 
# MAGIC 1. Partition by Service Zone  

# COMMAND ----------

Vendor_Df.write.format("parquet") \
                  .partitionBy("vendor_id") \
                  .mode("Overwrite") \
                  .parquet(f"{DB_PROCESSED}/vendor")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3 - Create Database
# MAGIC #####Please look into 01_Nyc_Trip_Taxi_Zone_Ingestion  Notebook 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 4 - Creating Temp and Global views

# COMMAND ----------

Vendor_Df.createOrReplaceTempView("newyork_taxi.vendor")
Vendor_Df.createOrReplaceGlobalTempView("newyork_taxi.vendor_g")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 5 - Storing data as Hive Managed Table

# COMMAND ----------

Vendor_Df.write.format("parquet") \
                  .partitionBy("vendor_id") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable("newyork_taxi.vendor_int")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.vendor_int

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 6 - Storing data as Hive EXTERNAL Table

# COMMAND ----------

Vendor_Df.write.format("parquet") \
                  .partitionBy("vendor_id") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_PROCESSED}/vendor_ext" ) \
                  .saveAsTable("newyork_taxi.vendor_ext")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.vendor_ext
