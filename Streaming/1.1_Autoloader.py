# Databricks notebook source
# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %md 
# MAGIC **STREAMING DONT WORK WITHOUT SCHEMA**

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Without InferSchema just Schema Location

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
           .option("cloudFiles.format" , "csv") \
            .option("cloudFiles.schemaLocation" , "/mnt/covidreportingdatalake6/streamwrite/schema/auto") \
           .load("/mnt/covidreportingdatalake6/streamread")\
            .writeStream\
            .option("checkpointLocation" ,f"{DB_STREAMCHECKPOINT}/auto")\
            .option("path" , "/mnt/covidreportingdatalake6/streamwrite/auto")\
            .table("newyork_taxi.autoloader1")

# COMMAND ----------

# MAGIC %sql select * from  newyork_taxi.autoloader1

# COMMAND ----------

# MAGIC %md 
# MAGIC ##InferSchema 

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
           .option("cloudFiles.format" , "csv") \
             .option("cloudFiles.inferColumnTypes" , True)\
            .option("cloudFiles.schemaLocation" , "/mnt/covidreportingdatalake6/streamwrite/schema/auto2") \
           .load("/mnt/covidreportingdatalake6/streamread")\
            .writeStream\
            .option("checkpointLocation" ,f"{DB_STREAMCHECKPOINT}/auto2")\
            .option("path" , "/mnt/covidreportingdatalake6/streamwrite/auto2")\
            .table("newyork_taxi.autoloader2")

# COMMAND ----------

# MAGIC %sql select * from newyork_taxi.autoloader2

# COMMAND ----------

# MAGIC %md 
# MAGIC **MERGE SCHEMA Manually, We need to start the job  **

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
           .option("cloudFiles.format" , "csv") \
             .option("cloudFiles.inferColumnTypes" , True)\
            .option("cloudFiles.schemaLocation" , "/mnt/covidreportingdatalake6/streamwrite/schema/auto2") \
           .load("/mnt/covidreportingdatalake6/streamread")\
            .writeStream\
            .option("checkpointLocation" ,f"{DB_STREAMCHECKPOINT}/auto2")\
            .option("path" , "/mnt/covidreportingdatalake6/streamwrite/auto2")\
            .option("mergeSchema" , True) \
            .table("newyork_taxi.autoloader2")

# COMMAND ----------

# MAGIC %sql select * from newyork_taxi.autoloader2

# COMMAND ----------

# MAGIC %md 
# MAGIC ** ADDING NEW SCHEMA ,Stream should not stop 

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
           .option("cloudFiles.format" , "csv") \
             .option("cloudFiles.inferColumnTypes" , True)\
                 .option("cloudFiles.schemaEvolutionMode" , "addNewColumns") \
            .option("cloudFiles.schemaLocation" , "/mnt/covidreportingdatalake6/streamwrite/schema/auto3") \
           .load("/mnt/covidreportingdatalake6/streamread")\
            .writeStream\
            .option("checkpointLocation" ,f"{DB_STREAMCHECKPOINT}/auto3")\
            .option("path" , "/mnt/covidreportingdatalake6/streamwrite/auto3")\
            .option("mergeSchema" , True) \
            .table("newyork_taxi.autoloader3")

# COMMAND ----------

# MAGIC %sql SELECT * FROM newyork_taxi.autoloader3
