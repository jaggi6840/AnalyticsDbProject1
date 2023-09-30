# Databricks notebook source
# MAGIC %md 
# MAGIC #*INGEST RATE CODE FILE*

# COMMAND ----------

dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 1 - Reading Rate Code Json  File from ADLS**
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC

# COMMAND ----------

Rate_Code_Df = spark.read.format("json") \
                     .schema(Rate_Code_schema) \
                     .option("multiline" , True) \
                     .load(f"{DB_RAW}/raw/rate_code.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 1 a - Reading Rate Code MULTI Json  File from ADLS**
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  

# COMMAND ----------

Rate_Code_ML_Df = spark.read.format("json") \
                              .option("multiline" , True) \
                             .load(f"{DB_RAW}/raw/rate_code_multi_line.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 3 - Create Database**
# MAGIC #####Please look into 01_Nyc_Trip_Taxi_Zone_Ingestion  Notebook 

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 4 - Creating Temp and Global views**

# COMMAND ----------

Rate_Code_ML_Df.createOrReplaceTempView("newyork_taxi.rate_code")
Rate_Code_ML_Df.createOrReplaceGlobalTempView("newyork_taxi.rate_code_g")

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 5 - Storing data as Hive Managed Table**

# COMMAND ----------

Rate_Code_ML_Df.write.format("parquet") \
                  .partitionBy("rate_code") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable("newyork_taxi.rate_code_int")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.rate_code_int

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 6 - Storing data as Hive EXTERNAL Table**

# COMMAND ----------

Rate_Code_ML_Df.write.format("parquet") \
                  .partitionBy("rate_code") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_PROCESSED}/rate_code_ext" ) \
                  .saveAsTable("newyork_taxi.rate_code_ext")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.rate_code_ext
