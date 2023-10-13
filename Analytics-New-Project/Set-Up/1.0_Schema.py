# Databricks notebook source
# MAGIC %md 
# MAGIC **Schema Declaration**
# MAGIC 1. Notebook will be used to store all the schema

# COMMAND ----------

DB_RAW='/mnt/analyticsdbhub/raw'
DB_PROCESSED='/mnt/analyticsdbhub/processed'
DB_LOOKUP='/mnt/analyticsdbhub/lookup'
DB_ERROR='/mnt/analyticsdbhub/error'
DB_STREAMWRITE='/mnt/analyticsdbhub/streamwrite'
DB_STREAMREAD='/mnt/analyticsdbhub/streamread'
DB_STREAMCHECKPOINT='/mnt/analyticsdbhub/streamcheckpoint'
DB_DELTA='/mnt/analyticsdbhub/delta'
DB_BRONZE='/mnt/analyticsdbhub/bronze'
DB_SILVER='/mnt/analyticsdbhub/silver'
DB_GOLD='/mnt/analyticsdbhub/gold'

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType
countries_schema = StructType([
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
