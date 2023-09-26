# Databricks notebook source
# MAGIC %md
# MAGIC   |Table|Content|
# MAGIC   |--|--|
# MAGIC   |01|This NoteBook will be used to Paramterised File Date |
# MAGIC   |02|Use magic run Command to evaluate Parameters |
# MAGIC   |03|Read File with InferSchema |
# MAGIC   |04|Read File with Schema in a file  |
# MAGIC   |04|Recursive File lookup to find all the files   |
# MAGIC   |04|Use File Path to append for vairous files for Reconciliation purposes   |
# MAGIC   |04|Read File with Schema in a file  |

# COMMAND ----------

dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/parameters"

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 1 - Reading trip data green parquet File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC 1. Recursive File 
# MAGIC 1. file Path to find all th files 

# COMMAND ----------

from pyspark.sql.functions import input_file_name,count,col
Green_Trip_DF = spark.read.format("parquet") \
                     .option("InferSchema" , True)\
                     .option("Header" , True) \
                     .load(f"{DB_RAW}/raw/trip_data_green_parquet/") \
                      .withColumn("File_Name" , input_file_name())
display(Green_Trip_DF)

# COMMAND ----------

# from pyspark.sql.functions import input_file_name,count,col
# Green_Trip_DF = spark.read.format("parquet") \
#                      .option("InferSchema" , True)\
#                      .option("Header" , True) \
#                      .load(f"{DB_RAW}/raw/trip_data_green_parquet/") \
#                       .withColumn("File_Name" , input_file_name())
# #Green_Trip_DF.groupBy(col('File_Name')).agg(count(col('*')))
# display(Green_Trip_DF)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2 - Reading trip data in a Temp View (Old Way)

# COMMAND ----------

Green_Trip_DF.createOrReplaceTempView('Green_Trip_TEMP')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 3 - Reading trip data from Dataframe (New Way)

# COMMAND ----------

sqlDf = spark.sql("select * from {table}", table=Green_Trip_DF)

# COMMAND ----------

spark.sql("Select payment_type, count(*) from Green_Trip_TEMP \
                                 Where Year = 2020 and month=1 Group by payment_type" ).show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * from global_temp.payment_type_g

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 3 - Create Database
# MAGIC #####Please look into 01_Nyc_Trip_Taxi_Zone_Ingestion  Notebook 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 - Creating Temp and Global views

# COMMAND ----------

Green_Trip_DF.createOrReplaceTempView("newyork_taxi.green_trip")
Green_Trip_DF.createOrReplaceGlobalTempView("newyork_taxi.green_trip_g")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Storing data as Hive Managed Table

# COMMAND ----------

Green_Trip_DF.write.format("parquet") \
                  .partitionBy("year","month") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable("newyork_taxi.green_trip_int")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED newyork_taxi.green_trip_int

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 6 - Storing data as Hive EXTERNAL Table

# COMMAND ----------

Green_Trip_DF.write.format("parquet") \
                  .partitionBy("year","month") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_PROCESSED}/green_trip_ext" ) \
                  .saveAsTable("newyork_taxi.green_trip_ext")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED newyork_taxi.green_trip_ext

# COMMAND ----------

# MAGIC %sql 
# MAGIC REFRESH TABLE  newyork_taxi.green_trip_ext
