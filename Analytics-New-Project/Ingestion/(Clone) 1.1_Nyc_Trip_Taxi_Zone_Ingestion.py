# Databricks notebook source
# MAGIC %md 
# MAGIC #*INGEST TAXI ZONE FILE*

# COMMAND ----------

dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# %sql SELECT * FROM DELTA.daily_log1

# COMMAND ----------

# df = spark.sql("SELECT * FROM DELTA.daily_log1 order by day desc limit 1")
# year = int(df.collect()[0][0])
# month=int(df.collect()[0][1])
# day= int(df.collect()[0][2]) - 1

# COMMAND ----------

# dbutils.fs.ls(f"/mnt/covidreportingdatalake6/raw/daily/{year}/{month}/{day}/")

# COMMAND ----------

dbutils.fs.ls(f"/mnt/covidreportingdatalake6/raw/daily/")

# COMMAND ----------



# COMMAND ----------

def get_latest_file(path):
     list_of_file=(dbutils.fs.ls(path))
     latest_file= [i.name for i in list_of_file ]
     return latest_file


# COMMAND ----------

path= '/mnt/covidreportingdatalake6/raw/daily/'
latest_file_name = get_latest_file(path)


latest_file_path=[]
for file_name in list(latest_file_name):
    latest_file_path.append(path+file_name)
print(latest_file_path)

latest_file_path

# COMMAND ----------

from pyspark.sql.functions import input_file_name
# Schema is stored in set-up/Schema"
from pyspark.sql.functions import col
Taxi_Zone_Df = spark.read.format("csv") \
                     .option("InferSchema" , True)\
                     .option("Header" , True)\
                     .option("recursiveFileLookup",True) \
                     .load(latest_file_path) \
                     .withColumn("File_Name" , input_file_name())


# COMMAND ----------

Taxi_Zone_Df1= Taxi_Zone_Df.select((col('File_Name'))).dropDuplicates()
display(Taxi_Zone_Df1)

# COMMAND ----------

for i in list(latest_file_path):
    dbutils.fs.mv(i  ,  '/mnt/covidreportingdatalake6/raw/processed')
   #mv file_name /mnt/covidreportingdatalake6/raw/daily/mv

# COMMAND ----------

Taxi_Zone_Df1.write.format("parquet") \
                    .mode("OVERWRITE") \
                     .saveAsTable("DELTA.daily_log1")

# COMMAND ----------

from pyspark.sql.functions import split,split
Taxi_Zone_Df_file = Taxi_Zone_Df1.withColumn("fi" ,split(col('File_name'),'/')[1]).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split,split
Taxi_Zone_Df_file = Taxi_Zone_Df1.withColumn("year" ,split(col('File_name'),"/")[:-1]) \
             .withColumn("month" ,split(col('File_name'),"/")[-1]) \
             .withColumn("day" ,split(col('File_name'),"/")[-1]) \
             .withColumn("file_name" ,split(col('File_name'),"/")[-1]).show()
            #  .select(col('year'),col('month'),col('day'),col('file_name'))

# COMMAND ----------

Taxi_Zone_Df_file.write.format("parquet") \
                    .mode("APPEND") \
                     .saveAsTable("DELTA.daily_log1")

                #      Taxi_Zone_Df.write.format("parquet") \
                #   .partitionBy("Service_Zone") \
                #   .mode("Overwrite") \
                #   .option("comments","This is a EXTERNAL Table ") \
                #   .option("path", f"{DB_PROCESSED}/taxi_zone_ext" ) \
                #   .saveAsTable("newyork_taxi.taxi_zone_ext")

# COMMAND ----------


# Athlete_df_name = Athlete_df_req.withColumn("First_name" , initcap(split(col("name") , " ")[0])) \
#               .withColumn("Middle_name" , initcap(split(col("name") , " ")[2])) \
#               .withColumn("Last_name" , initcap(split(col("name") , " ")[1])) 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 1 a - Reading Taxi Zone File Without Header from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema   

# COMMAND ----------

# Schema is stored in set-up/Schema"
Taxi_Zone_Df = spark.read.format("csv") \
                     .schema(Taxi_Zone_Schema)\
                     .load(f"{DB_RAW}/raw/taxi_zone_without_header.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 2 - Writing  Taxi Zone Parquet File 
# MAGIC 1. Partition by Service Zone  

# COMMAND ----------

Taxi_Zone_Df.write.format("parquet") \
                  .partitionBy("Service_Zone") \
                  .mode("Overwrite") \
                  .parquet(f"{DB_PROCESSED}/Taxi_Zone")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3 - Create Database
# MAGIC 1. USE %sql to convert python to sql   

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3a - Create Database
# MAGIC 1. Create Database  

# COMMAND ----------

# %sql Create  Database NEWYORK_TAXI

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3b - Create Database
# MAGIC 1. Show Databases

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3c - Change Database
# MAGIC 1. Change Database 

# COMMAND ----------

# %sql 
# USE newyork_taxi;

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3d - Show Current Database
# MAGIC 1. Change Database 

# COMMAND ----------

# %sql 
# SELECT CURRENT_DATABASE()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 4 - Creating Temp and Global views

# COMMAND ----------

Taxi_Zone_Df.createOrReplaceTempView("newyork_taxi.taxi_zone")
Taxi_Zone_Df.createOrReplaceGlobalTempView("hive_metastore.newyork_taxi.autoloader1.taxi_zone_g")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 5 - Storing data as Hive Managed Table

# COMMAND ----------

Taxi_Zone_Df.write.format("parquet") \
                  .partitionBy("Service_Zone") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable("newyork_taxi.taxi_zone_int")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.taxi_zone_int

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 6 - Storing data as Hive EXTERNAL Table

# COMMAND ----------

Taxi_Zone_Df.write.format("parquet") \
                  .partitionBy("Service_Zone") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_PROCESSED}/taxi_zone_ext" ) \
                  .saveAsTable("newyork_taxi.taxi_zone_ext")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.taxi_zone_ext
