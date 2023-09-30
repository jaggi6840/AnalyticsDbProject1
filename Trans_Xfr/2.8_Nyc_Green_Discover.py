# Databricks notebook source
dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 1 - Reading trip data green parquet File from PROCESSED**
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import input_file_name,count,col
Green_Trip_DF1 = spark.read.format("parquet") \
                     .option("InferSchema" , True)\
                     .option("Header" , True) \
                     .load(f"{DB_PROCESSED}/green_trip_ext/")

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
# MAGIC **Step 1a - Doing Filter , Drop , Select, Duplicates**
# MAGIC   |Table|Content|
# MAGIC   |--|--|
# MAGIC   |01|For Multiple drops you don't need COL FUNCTIONS|
# MAGIC 1. For Multiple drops you don't need COL FUNCTIONS| 
# MAGIC 1. DROP_DUPLICATES
# MAGIC 1. Drop all fields 
# MAGIC 1. Select 
# MAGIC 1. where .isNull filter (string and integer are different)
# MAGIC

# COMMAND ----------

Green_Trip_DF = Green_Trip_DF1.filter(col('lpep_dropoff_datetime') > col('lpep_pickup_datetime')) \
                              .drop('lpep_dropoff_datetime','lpep_pickup_datetime','input_file_name') \
                              .drop(*['store_and_fwd_flag']) \
                              .select(col('VendorID'),col('RatecodeID'),col('PULocationID'),col('DOLocationID') \
                              ,col('trip_distance'),col('fare_amount'),col('extra'),col('mta_tax'),col('tip_amount'),col('tolls_amount'),col('ehail_fee'),col('improvement_surcharge')\
                              ,col('total_amount'),col('payment_type'),col('trip_type'),col('year'),col('month')).drop_duplicates() \
                              .filter(col('ehail_fee').isNull()) \
                              .na.fill(0) 

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

spark.sql("Select count(*)  from Green_Trip_TEMP \
                                 Where Year = 2021 and month=1 and payment_type in (1,2) " ).show()

# COMMAND ----------

# %sql 
# SELECT * from global_temp.payment_type_g

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 4 - Filter  Green trip  data**
# MAGIC 1. Filter data for year 2020 and Month = 1

# COMMAND ----------

 Green_Trip_Filter_DF = Green_Trip_DF

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 5 - Taxi Zone Data**
# MAGIC 1. Create dataframe from Previously created Notebook for Taxi Zone, We can use temp global, managed,external  
# MAGIC

# COMMAND ----------

Taxi_Zone_Df = spark.sql("Select * from newyork_taxi.taxi_zone_ext ")

# COMMAND ----------

# MAGIC %md 
# MAGIC **Step 6 - Payment Type**
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

Green_Taxi_Zone_Payment_Df = Green_Taxi_Zone_Join_Df.alias('GT').join(Taxi_Zone_Df.alias('TZ') , col('GT.PULocationID') == col('TZ.LocationID') ).drop_duplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 6 -Find No of Trips for 2021 in January**

# COMMAND ----------

from pyspark.sql.functions import desc 
Green_Taxi_Zone_Payment_Df1= Green_Taxi_Zone_Payment_Df.groupBy(col('Year'),col('Month'),col('Borough'),col('value')) \
                             .agg(count('value').alias('Total_Trips')).orderBy(col('Borough'))
