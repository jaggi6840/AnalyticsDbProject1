# Databricks notebook source
dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %run "../set-up/parameters"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 1 - Reading File from Taxi Zone File ADLS
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema    
# MAGIC

# COMMAND ----------

# Schema is stored in set-up/Schema"
from pyspark.sql.functions import col
Taxi_Zone_Df = spark.read.format("csv") \
                     .option("InferSchema" , True)\
                     .option("Header" , True)\
                     .load(f"{DB_RAW}/raw/taxi_zone.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 1 a - Reading Taxi Zone File Without Header from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema   

# COMMAND ----------

# Schema is stored in set-up/Schema"
Taxi_Zone_Df = spark.read.format("csv") \
                     .schema(Taxi_Zone_Schema)\
                     .load(f"{DB_RAW}/raw/taxi_zone_without_header.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC #####  Removing Duplicates as total

# COMMAND ----------

Taxi_Zone_Df.dropDuplicates()

# COMMAND ----------

# MAGIC %md 
# MAGIC #####  Removing Duplicates on Location Id 

# COMMAND ----------

from pyspark.sql.functions import col,count
Taxi_Zone_Df.groupBy(col('LocationID')).agg(count(col('LocationID')).alias("Total")).orderBy(col('Total').desc())

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2 - Reading Calendar  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema   

# COMMAND ----------

Calendar_Df = spark.read.format("csv") \
                     .schema(Calendar_Schema)\
                     .option("Header" , True) \
                     .load(f"{DB_RAW}/raw/calendar.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 3 - Reading Vendor  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC 1. Header is true as the file has header

# COMMAND ----------

Vendor_Df = spark.read.format("csv") \
                     .schema(Vendor_Schema)\
                     .option("Header" , True) \
                     .load(f"{DB_RAW}/raw/vendor_escaped.csv").show(truncate= False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 - Reading Trip Type  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC 1. Header is true as the file has header
# MAGIC 1. This file is a tab seprated file

# COMMAND ----------

Trip_Type_Df = spark.read.format("csv") \
                     .schema(Trip_Type_Schema)\
                     .option("sep" , "\t") \
                     .option("Header" , True) \
                     .load(f"{DB_RAW}/raw/trip_type.tsv").show(truncate= False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Reading Rate Code Json  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC

# COMMAND ----------

Rate_Code_Df = spark.read.format("json") \
                     .schema(Rate_Code_schema) \
                     .option("multiline" , True) \
                     .load(f"{DB_RAW}/raw/rate_code.json").show(truncate= False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 5 a - Reading Rate Code MULTI Json  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  

# COMMAND ----------

Rate_Code_ML_Df = spark.read.format("json") \
                              .option("multiline" , True) \
                             .load(f"{DB_RAW}/raw/rate_code_multi_line.json").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 6 - Reading Payment Type Json  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  

# COMMAND ----------

Payment_Type_Df = spark.read.format("json") \
    .schema(Payment_Type_schema) \
                     .load(f"{DB_RAW}/raw/payment_type.json").show(truncate= False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 6 - Reading Payment Type Array Json  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC 1. EXPLODE function to read file 

# COMMAND ----------

from pyspark.sql.functions import explode,explode_outer,posexplode_outer
Payment_Type_Array_Df = spark.read.format("json") \
                             .load(f"{DB_RAW}/raw/payment_type_array.json")

# COMMAND ----------

Payment_Type_Array_Df1= Payment_Type_Array_Df.withColumn("topping_explode",explode("payment_type_desc"))\
                                               .withColumn("sub_type",col("topping_explode.sub_type")) \
                                                   .withColumn("value",col("topping_explode.value")) \
                                                   .drop(col('payment_type_desc')) \
                                                   .drop(col('topping_explode'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 8 - Reading trip data green  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC 1. Recursive File 
# MAGIC 1. * file Path

# COMMAND ----------

from pyspark.sql.functions import input_file_name,count
Green_Trip_DF = spark.read.format("csv") \
                     .schema(Green_Trip_Schema)\
                     .option("Header" , True) \
                      .option("recusrsiveFileLookup" , True) \
                     .load(f"{DB_RAW}/raw/trip_data_green_csv/year=2020") \
                      .withColumn("File_Name" , input_file_name())
Green_Trip_DF.groupBy(col('File_Name')).agg(count(col('*'))).show(truncate = False)


# COMMAND ----------


