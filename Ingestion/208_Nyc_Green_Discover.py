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
# MAGIC ### Step 1 - Reading trip data green parquet File from PROCESSED 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import input_file_name,count,col
Green_Trip_DF = spark.read.format("parquet") \
                     .option("InferSchema" , True)\
                     .option("Header" , True) \
                     .load(f"{DB_PROCESSED}/green_trip_ext/") \
                      .withColumn("File_Name" , input_file_name())\
                      .drop(col('File_Name'))
#Green_Trip_DF.groupBy(col('File_Name')).agg(count(col('*')))
display(Green_Trip_DF)

# COMMAND ----------

# from pyspark.sql.functions import input_file_name,count,col
# Green_Trip_DF = spark.read.format("parquet") \
#                      .option("InferSchema" , True)\
#                      .option("Header" , True) \
#                      .load(f"{DB_PROCESSED}/green_trip_ext/") \
#                       .withColumn("File_Name" , input_file_name())\
#                       .select(col('VendorID') ,col('lpep_pickup_datetime'),col('lpep_dropoff_datetime'), col('PULocationID')  , col('year') , col('month'), col('payment_type'), col('trip_type'))\
#                           .filter((col('year')==2020) & (col('month')==1))
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

spark.sql("Select count(*)  from Green_Trip_TEMP \
                                 Where Year = 2021 and month=1 and payment_type in (1,2) " ).show()

# COMMAND ----------

# %sql 
# SELECT * from global_temp.payment_type_g

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 - Filter  Green trip  data 
# MAGIC 1. Filter data for year 2020 and Month = 1

# COMMAND ----------

Green_Trip_Filter_DF = Green_Trip_DF.filter((col('year')==2021) & (col('month')==1))
display(Green_Trip_Filter_DF)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Taxi Zone Data 
# MAGIC 1. Create dataframe from Previously created Notebook for Taxi Zone, We can use temp global, managed,external  
# MAGIC

# COMMAND ----------

Taxi_Zone_Df = spark.sql("Select * from newyork_taxi.taxi_zone_ext ")
display(Taxi_Zone_Df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 6 - Payment Type
# MAGIC 1. Create dataframe from Previously created Notebook for Payment Type, We can use temp global, managed,external  
# MAGIC 1. Filter Payment Type for Cash and Credit Card 

# COMMAND ----------

# %sql SELECT * FROM GLOBAL_TEMP.payment_type_g

# COMMAND ----------

Payment_Type_Df = spark.sql("SELECT * FROM newyork_taxi.payment_type_ext").filter((col('value')=='Credit card') |  (col('value')=='Cash'))
#sqlDf = spark.sql("select * from {table}", table=Green_Trip_DF)

# COMMAND ----------

#Green_Taxi_Zone_Join_Df = Green_Trip_Filter_DF.alias('GT').join(Taxi_Zone_Df.alias('TZ') , col('GT.PULocationID') == col('TZ.LocationID') ).drop_duplicates()
#.filter((col('Borough') == 'Unknown') & ((col('payment_type')==1)) | (col('payment_type')==2))
#Green_Taxi_Zone_Join_Df1 = Green_Taxi_Zone_Join_Df.select(col('payment_type')==1 | col('payment_type')==2)
Green_Taxi_Zone_Join_Df = Green_Trip_Filter_DF.alias('GT').join( Payment_Type_Df.alias('PT') , col('GT.payment_type')  == col('PT.payment_type')  ,'inner'   ).select(col('GT.*'),col('PT.value'))
Green_Taxi_Zone_Join_Df.count()

Green_Taxi_Zone_Payment_Df = Green_Taxi_Zone_Join_Df.alias('GT').join(Taxi_Zone_Df.alias('TZ') , col('GT.PULocationID') == col('TZ.LocationID') ).filter(col('lpep_dropoff_datetime') > col('lpep_pickup_datetime')).drop_duplicates()
display(Green_Taxi_Zone_Join_Df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 -Find No of Trips for 2021 in January 

# COMMAND ----------

from pyspark.sql.functions import desc 
Green_Taxi_Zone_Payment_Df1= Green_Taxi_Zone_Payment_Df.groupBy(col('Borough'),col('value')) \
                             .agg(count('value').alias('Total_Trips')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - NOT WORKING 
