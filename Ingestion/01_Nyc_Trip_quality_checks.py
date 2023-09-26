# Databricks notebook source
dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %run "../set-up/parameters"

# COMMAND ----------

# MAGIC %md 
# MAGIC Reading trip data green parquet File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC 1. Recursive File 
# MAGIC 1. * file Path

# COMMAND ----------

from pyspark.sql.functions import input_file_name,count,col
Green_Trip_DF = spark.read.format("parquet") \
                     .schema(Green_Trip_Schema)\
                     .option("Header" , True) \
                     .load(f"{DB_RAW}/raw/trip_data_green_parquet/year=2020") \
                    .withColumn("File_Name" , input_file_name()).show()


# COMMAND ----------

Green_Trip_DF.groupBy(col('File_Name')).agg(count(col('*'))).show(truncate = False)

# COMMAND ----------

Green_Trip_DF.groupBy(col('File_Name')).agg()
