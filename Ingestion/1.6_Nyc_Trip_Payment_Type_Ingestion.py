# Databricks notebook source
# MAGIC %md 
# MAGIC #*INGEST PAYMENT TYPE FILE*

# COMMAND ----------

dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 6 - Reading Payment Type Json  File from ADLS** 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  

# COMMAND ----------

Payment_Type_Df = spark.read.format("json") \
                             .schema(Payment_Type_schema) \
                             .load(f"{DB_RAW}/raw/payment_type.json").show(truncate= False)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 6 - Reading Payment Type Array Json  File from ADLS**
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC 1. EXPLODE function to read file
# MAGIC 1 .Filter Subtype 1 

# COMMAND ----------

from pyspark.sql.functions import explode,explode_outer,posexplode_outer
Payment_Type_Array_Df = spark.read.format("json") \
                             .load(f"{DB_RAW}/raw/payment_type_array.json")

# COMMAND ----------

from pyspark.sql.functions import col
Payment_Type_Array_Df1= Payment_Type_Array_Df.withColumn("topping_explode",explode("payment_type_desc"))\
                                               .withColumn("sub_type",col("topping_explode.sub_type")) \
                                                   .withColumn("value",col("topping_explode.value")) \
                                                   .drop(col('payment_type_desc')) \
                                                   .drop(col('topping_explode')) \
                                                    .filter(col('sub_type')==1)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 3 - Create Database**
# MAGIC #####Please look into 01_Nyc_Trip_Taxi_Zone_Ingestion  Notebook 

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 4 - Creating Temp and Global views**

# COMMAND ----------

Payment_Type_Array_Df1.createOrReplaceTempView("newyork_taxi.payment_type")
Payment_Type_Array_Df1.createOrReplaceGlobalTempView("newyork_taxi.payment_type_g")

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 5 - Storing data as Hive Managed Table**

# COMMAND ----------

Payment_Type_Array_Df1.write.format("parquet") \
                  .partitionBy("payment_type") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable("newyork_taxi.payment_type_int")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.payment_type_int

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 6 - Storing data as Hive EXTERNAL Table**

# COMMAND ----------

Payment_Type_Array_Df1.write.format("parquet") \
                  .partitionBy("payment_type") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_PROCESSED}/payment_type_ext" ) \
                  .saveAsTable("newyork_taxi.payment_type_ext")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.payment_type_ext
