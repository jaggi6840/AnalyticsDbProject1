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

# MAGIC %run "../set-up/parameters"

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 1 - Reading trip data green parquet File from PROCESSED**
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import input_file_name,count,col
Green_Trip_DF = spark.read.format("parquet") \
                     .option("InferSchema" , True)\
                     .option("Header" , True) \
                     .load(f"{DB_PROCESSED}/green_trip_ext/") \
                      .withColumn("File_Name" , input_file_name())
#Green_Trip_DF.groupBy(col('File_Name')).agg(count(col('*')))
display(Green_Trip_DF)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 2 - Reading trip data in a Temp View (Old Way)**

# COMMAND ----------

Green_Trip_DF.createOrReplaceTempView('Green_Trip_TEMP')

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 3 - Reading trip data from Dataframe (New Way)**

# COMMAND ----------

sqlDf = spark.sql("select * from {table}", table=Green_Trip_DF)

# COMMAND ----------

spark.sql("Select *  from Green_Trip_TEMP \
                                 Where Year = 2020 and month=1 AND PULocationID is NULL" )

# COMMAND ----------

# %sql 
# SELECT * from global_temp.payment_type_g

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 4 - Filter  Green trip  data**
# MAGIC 1. Filter data for year 2020 and Month = 1

# COMMAND ----------

Green_Trip_Filter_DF = Green_Trip_DF.filter((col('year')==2020) & (col('month')==1))

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 5 - Taxi Zone Data**
# MAGIC 1. Create dataframe from Previously created Notebook for Taxi Zone, We can use temp global, managed,external  
# MAGIC

# COMMAND ----------

Taxi_Zone_Df = spark.sql("Select * from newyork_taxi.taxi_zone_ext")

# COMMAND ----------

Green_Taxi_Zone_Join_Df = Green_Trip_Filter_DF.alias('GT').join(Taxi_Zone_Df.alias('TZ') , col('GT.PULocationID') == col('TZ.LocationID'))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6 -Find No of Trips for 2020 in January**

# COMMAND ----------

from pyspark.sql.functions import desc 
Green_Taxi_Zone_Join_Df.groupBy(col('Borough')).agg(count('*').alias('No Of Trips')).orderBy(col('No Of Trips').asc())

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 7 -Find No of hours for each trip**

# COMMAND ----------

from pyspark.sql.functions import datediff,year,unix_timestamp
Green_Trip_hr_DF = Green_Trip_Filter_DF.withColumn("seconds_between",(unix_timestamp(col('lpep_dropoff_datetime')) - unix_timestamp(col('lpep_pickup_datetime')))).select(col('lpep_pickup_datetime'),col('lpep_dropoff_datetime'),col('seconds_between'))\
    .withColumn("minutes_between" , (col('seconds_between')/60).cast('bigint')) \
    .withColumn("from_hour" , (col('minutes_between')/60).cast('int')) \
    .withColumn("to_hour" , (col('minutes_between')/60).cast('int') + 1)


# COMMAND ----------

Green_Trip_hr_Count_DF = Green_Trip_hr_DF.groupby(col('from_hour'),col('to_hour')).agg(count(col('from_hour')).alias('Total_Trip')).orderBy(col('from_hour'))
display(Green_Trip_hr_Count_DF)
