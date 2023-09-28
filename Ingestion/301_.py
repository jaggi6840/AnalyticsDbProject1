# Databricks notebook source
dbutils.widgets.text("DB_NAME" , "")
DB_NAME1 = dbutils.widgets.get("DB_NAME")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### STEP 1 Create DATABASE DELTA TABLE 

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

# MAGIC %sql --SELECT * FROM DELTA.Taxi_Zone_Delta order by LocationId asc
# MAGIC INSERT INTO DELTA.Taxi_Zone_Delta(LocationID,Borough,Zone,Service_zone,active_status,start_date,end_date,delete_date) VALUES(260,'Queens','','Boro Zone','Y',NULL,NULL,NULL);
# MAGIC INSERT INTO DELTA.Taxi_Zone_Delta(LocationID,Borough,Zone,Service_zone,active_status,start_date,end_date,delete_date) VALUES(259,'Bronx','Woodlawn/Wakefield','Boro Zone','Y',NULL,NULL,NULL)
# MAGIC --DELETE FROM DELTA.Taxi_Zone_Delta where LOCATIONID in (259,257,258,259,260)

# COMMAND ----------

spark.sql(f"SELECT * FROM {DB_NAME1}.Taxi_Zone_Delta")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 Create DF from Table Created in 107 notebook

# COMMAND ----------

Taxi_Zone_Df = spark.sql("SELECT * FROM newyork_taxi.taxi_zone_ext where LocationID in (259,260,258,257)")

# COMMAND ----------

from delta.tables import *
deltaInstance1= DeltaTable.forPath(spark ,"/mnt/covidreportingdatalake6/delta/Taxi_Zone_Delta")


# COMMAND ----------

Tax_Zone_Delta1 = deltaInstance1.toDF()

# COMMAND ----------

# MAGIC %md  
# MAGIC #### STEP 3 
# MAGIC ##### Comaparing New and Delta tables to find READ INSERT UPDATES AND DELETES 

# COMMAND ----------

# MAGIC %md  
# MAGIC ###### STEP 3 a
# MAGIC ##### UPDATES From Source 

# COMMAND ----------

from pyspark.sql.functions import col
Tax_Zone_Delta= Tax_Zone_Delta1.filter(col('active_status') == 'Y')

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit,xxhash64
Taxi_Zone_Updates_Df = Taxi_Zone_Df.alias('S1').join(Tax_Zone_Delta.alias('T1') ,col('S1.LocationID') == col('T1.LocationID') , "inner" ).select(col('S1.*') \
                                                ,col('T1.Borough').alias('T1_Borough')\
                                                ,col('T1.Zone').alias('T1_Zone') \
                                                ,col('T1.Service_zone').alias('T1_Service_zone')\
                                                ,col('T1.active_status').alias('T1_active_status')\
                                                ,col('T1.start_date').alias('T1_start_date')\
                                                ,col('T1.end_date').alias('T1_end_date')\
                                                ,col('T1.delete_date').alias('T1_delete_date'))

# COMMAND ----------

display(Taxi_Zone_Updates_Df)

# COMMAND ----------

# MAGIC %md  
# MAGIC ###### STEP 3 b
# MAGIC ##### Filter Updates where data is not matching (means changes are there in the updates)

# COMMAND ----------

Taxi_Zone_Changes_Df = Taxi_Zone_Updates_Df.filter( xxhash64(col('Borough') ,col('Zone'),col('Service_zone') ) != xxhash64(col('T1_Borough'),col('T1_Zone'),col('T1_Service_zone')) ).select( col('LocationID'),col('Borough') ,col('Zone'),col('Service_zone'))

# COMMAND ----------

Taxi_Zone_Changes_Df.createOrReplaceGlobalTempView("Taxi_Zone_update_view")

# COMMAND ----------

# MAGIC %sql SELECT * FROM global_temp.Taxi_Zone_update_view

# COMMAND ----------

# MAGIC %md  
# MAGIC ###### STEP 3 c
# MAGIC ##### Create a new df to just have new value 

# COMMAND ----------

Taxi_Zone_Deltas_Df  = Taxi_Zone_Changes_Df.select( col('LocationID'),col('Borough') ,col('Zone'),col('Service_zone'))

# COMMAND ----------

# MAGIC %md  
# MAGIC #### STEP 4 
# MAGIC ##### Comaparing New and Delta tables to find READ INSERT 

# COMMAND ----------

Taxi_Zone_Insert_Df = Taxi_Zone_Df.alias('S1').join(Tax_Zone_Delta.alias('T1') ,  ((col('S1.LocationID') == col('T1.LocationID')) & (col('T1.active_status')=='Y' ) )   , "left_anti" ).select(col('S1.*'))

# COMMAND ----------

# MAGIC %md  
# MAGIC #### STEP 5 
# MAGIC ##### Adding Insert And Updates DataFrames

# COMMAND ----------

Taxi_Zone_Final_Df = Taxi_Zone_Deltas_Df.union(Taxi_Zone_Insert_Df)

# COMMAND ----------

# MAGIC %md  
# MAGIC #### STEP 6 
# MAGIC ##### Storing above data in a temp view

# COMMAND ----------

Taxi_Zone_Final_Df.createOrReplaceTempView("Taxi_Zone_Delta_view1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Taxi_Zone_Delta_view1

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO DELTA.Taxi_Zone_Delta as T1 
# MAGIC USING Taxi_Zone_Delta_view as S1
# MAGIC ON  T1.LocationID = S1.LocationID
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

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO DELTA.Taxi_Zone_Delta as T1 
# MAGIC USING Taxi_Zone_Delta_view as S1
# MAGIC ON  T1.LocationID = S1.LocationID
# MAGIC WHEN MATCHED AND active_status='Y'
# MAGIC THEN UPDATE 
# MAGIC SET 
# MAGIC active_status='N' , end_date=current_date()
# MAGIC WHEN MATCHED THEN 
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

# COMMAND ----------

# MAGIC %sql SELECT * from global_temp.Taxi_Zone_update_view

# COMMAND ----------

# MAGIC %sql SELECT * FROM Taxi_Zone_Delta_view

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO DELTA.Taxi_Zone_Delta (LocationID,
# MAGIC Borough,
# MAGIC Zone,
# MAGIC Service_zone,
# MAGIC active_status,
# MAGIC start_date,
# MAGIC end_date,
# MAGIC delete_date)
# MAGIC SELECT 
# MAGIC LocationID,
# MAGIC Borough,
# MAGIC Zone,
# MAGIC Service_zone,
# MAGIC 'Y',
# MAGIC current_date(),
# MAGIC NULL,
# MAGIC NULL
# MAGIC from global_temp.Taxi_Zone_update_view

# COMMAND ----------

# MAGIC %sql SELECT * FROM DELTA.Taxi_Zone_Delta 

# COMMAND ----------

#%sql TRUNCATE TABLE DELTA.Taxi_Zone_Delta 
