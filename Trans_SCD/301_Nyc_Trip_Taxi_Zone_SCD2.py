# Databricks notebook source
dbutils.widgets.text("DB_NAME" , "")
DB_NAME1 = dbutils.widgets.get("DB_NAME")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### STEP 1 Create DATABASE DELTA TABLE 

# COMMAND ----------

# MAGIC %sql
# MAGIC create  TABLE IF NOT EXISTS  DELTA.Taxi_Zone_Delta (
# MAGIC LocationID VARCHAR(50),
# MAGIC Borough VARCHAR(50),
# MAGIC Zone VARCHAR(50),
# MAGIC Service_zone VARCHAR(50),
# MAGIC active_status  VARCHAR(2),
# MAGIC start_date DATE,
# MAGIC end_date DATE,
# MAGIC delete_date DATE
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/covidreportingdatalake6/delta/Taxi_Zone_Delta'

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 2 Create DF from Table Created in 107 notebook

# COMMAND ----------

Taxi_Zone_Df = spark.sql("SELECT * FROM newyork_taxi.taxi_zone_ext")

# COMMAND ----------

from delta.tables import *
deltaInstance1= DeltaTable.forPath(spark ,"/mnt/covidreportingdatalake6/delta/Taxi_Zone_Delta")
Tax_Zone_Delta1 = deltaInstance1.toDF()

# COMMAND ----------

# MAGIC %md  
# MAGIC ###### STEP 3 Comaparing New and Delta tables to find READ INSERT UPDATES AND DELETES 
# MAGIC 1. Inner Join to find all the matching records where active flag = Y

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit,xxhash64
Tax_Zone_Delta= Tax_Zone_Delta1.filter(col('active_status') == 'Y')
Taxi_Zone_Updates_Df = Taxi_Zone_Df.alias('S1').join(Tax_Zone_Delta.alias('T1') ,col('S1.LocationID') == col('T1.LocationID') , "inner" ).select(col('S1.*') \
                                                ,col('T1.Borough').alias('T1_Borough')\
                                                ,col('T1.Zone').alias('T1_Zone') \
                                                ,col('T1.Service_zone').alias('T1_Service_zone')\
                                                ,col('T1.active_status').alias('T1_active_status')\
                                                ,col('T1.start_date').alias('T1_start_date')\
                                                ,col('T1.end_date').alias('T1_end_date')\
                                                ,col('T1.delete_date').alias('T1_delete_date'))

# COMMAND ----------

# MAGIC %md  
# MAGIC ###### STEP 3 a
# MAGIC 1. Filter real updates 
# MAGIC 1. Add Merge Key 

# COMMAND ----------

Taxi_Zone_Changes_Df1 = Taxi_Zone_Updates_Df.filter( xxhash64(col('Borough') ,col('Zone'),col('Service_zone') ) != xxhash64(col('T1_Borough'),col('T1_Zone'),col('T1_Service_zone')) ).select( col('LocationID'),col('Borough') ,col('Zone'),col('Service_zone'))
Taxi_Zone_Changes_Df2= Taxi_Zone_Changes_Df1.withColumn("merge_key" , lit(''))

# COMMAND ----------

# MAGIC %md  
# MAGIC #### STEP 4 Comapring New and Delta Using LEFT_ANTI JOIN
# MAGIC 1. Find all the Inserts 
# MAGIC 1. Merge key as Actual Key

# COMMAND ----------

Taxi_Zone_Insert_Df1 = Taxi_Zone_Df.alias('S1').join(Tax_Zone_Delta.alias('T1') ,  ((col('S1.LocationID') == col('T1.LocationID')) & (col('T1.active_status')=='Y' ) )   , "left_anti" ).select(col('S1.*'))
Taxi_Zone_Final_Df = Taxi_Zone_Insert_Df1.withColumn("merge_key" , col('LocationID'))
Taxi_Zone_changes_final_Df1 = Taxi_Zone_Changes_Df1.withColumn("merge_key" , col('LocationID'))

# COMMAND ----------

# MAGIC %md  
# MAGIC ###### STEP 5 Adding Insert And Updates DataFrames
# MAGIC 1. Union of Updates, Inserts , and updates(with Merge key as Null)

# COMMAND ----------

Taxi_Zone_Final_Df1 = Taxi_Zone_Final_Df.union(Taxi_Zone_changes_final_Df1).union(Taxi_Zone_Changes_Df2)

# COMMAND ----------

# MAGIC %md  
# MAGIC ###### STEP 6 Create Temp View for Insert Updates and Deletes

# COMMAND ----------

Taxi_Zone_Final_Df1.createOrReplaceTempView("Taxi_Zone_Final_Df")

# COMMAND ----------

# MAGIC %md  
# MAGIC ###### STEP 7 Use Merge statement to flag Historic record as Null Active as Y

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO DELTA.Taxi_Zone_Delta as T1 
# MAGIC USING Taxi_Zone_Final_Df as S1
# MAGIC ON  T1.LocationID = S1.merge_key
# MAGIC WHEN MATCHED AND active_status='Y'
# MAGIC THEN UPDATE 
# MAGIC SET 
# MAGIC active_status='N' , end_date=current_date()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (LocationID,
# MAGIC Borough,
# MAGIC Zone,
# MAGIC Service_zone,
# MAGIC active_status,
# MAGIC start_date,
# MAGIC end_date,
# MAGIC delete_date)
# MAGIC VALUES(
# MAGIC S1.LocationID,
# MAGIC S1.Borough,
# MAGIC S1.Zone,
# MAGIC S1.Service_zone,
# MAGIC 'Y',
# MAGIC current_date(),
# MAGIC '',
# MAGIC '')
# MAGIC
