# Databricks notebook source
# MAGIC %md 
# MAGIC **Streaming can be done from Cloud or normal folder**
# MAGIC 1.make sure to write .start()

# COMMAND ----------

DB_RAW='/mnt/analyticsdbhub/raw'
DB_PROCESSED='/mnt/analyticsdbhub/processed'
DB_LOOKUP='/mnt/analyticsdbhub/lookup'
DB_ERROR='/mnt/analyticsdbhub/error'
DB_STREAMWRITE='/mnt/analyticsdbhub/streamwrite'
DB_STREAMREAD='/mnt/analyticsdbhub/streamread'
DB_STREAMCHECKPOINT='/mnt/analyticsdbhub/streamcheckpoint'
DB_DELTA='/mnt/analyticsdbhub/delt'

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
country_schema = StructType([
         StructField("COUNTRY_ID" , IntegerType(),True),
         StructField("NAME" , StringType(),True),
         StructField("NATIONALITY" , StringType(),True),
         StructField("COUNTRY_CODE" , StringType(),True),
         StructField("ISO_ALPHA2" , StringType(),True),
         StructField("CAPITAL" , StringType(),True),
         StructField("POPULATION" , IntegerType(),True),
         StructField("AREA_KM2" , DoubleType(),True),
         StructField("REGION_ID" , IntegerType(),True),
         StructField("SUB_REGION_ID" , IntegerType(),True),
         StructField("INTERMEDIATE_REGION_ID" , IntegerType(),True),
         StructField("ORGANIZATION_REGION_ID" , IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md 
# MAGIC **READ COUNTRIES CSV FILE FROM ADLS**

# COMMAND ----------

fulldf = spark.read.format("csv")\
                .schema(country_schema)\
                 .option("header",True)\
                .load(f"{DB_STREAMREAD}/countries.csv")
display(fulldf)                

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS STREAM;
# MAGIC SHOW  DATABASES;
# MAGIC --USE DELTA;
# MAGIC SELECT current_database()

# COMMAND ----------

fulldf.write.format("parquet")\
         .mode("Overwrite")\
         .save(f"{DB_PROCESSED}/countries/full")
# df= fulldf.limit(10)
# df.write.format("parquet")\
#          .mode("Overwrite")\
#          .save(f"{DB_PROCESSED}/countries/small")


# COMMAND ----------

dbutils.fs.ls('/mnt/analyticsdbhub/processed/')

# COMMAND ----------

# MAGIC %sql 
# MAGIC create  STREAM.full using delta location '/mnt/analyticsdbhub/processed/countries/full'
# MAGIC --create table STREAM.small using delta location '/mnt/analyticsdbhub/processed/countries/small'

# COMMAND ----------

# MAGIC %sql SELECT COUNT(*) FROM STREAM.full

# COMMAND ----------

# MAGIC %sql SELECT count(*) FROM STREAM.small
