# Databricks notebook source
# MAGIC %md
# MAGIC ###### Creating Parameters for all the containers
# MAGIC

# COMMAND ----------

DB_RAW='/mnt/analyticsdbhub/raw'
DB_PROCESSED='/mnt/analyticsdbhub/processed'
DB_LOOKUP='/mnt/analyticsdbhub/lookup'
DB_ERROR='/mnt/analyticsdbhub/error'
DB_STREAMWRITE='/mnt/analyticsdbhub/streamwrite'
DB_STREAMREAD='/mnt/analyticsdbhub/streamread'
DB_STREAMCHECKPOINT='/mnt/analyticsdbhub/streamcheckpoint'
DB_DELTA='/mnt/analyticsdbhub/delt'


print(f"DB_Raw is : {DB_RAW}" )
print(f"DB_PROCESSED is : {DB_PROCESSED}" )
print(f"DB_LOOKUP is : {DB_LOOKUP}" )
print(f"DB_ERROR is : {DB_ERROR}" )
print(f"DB_STREAMWRITE is : {DB_STREAMWRITE}" )
print(f"DB_ERROR1 is : {DB_ERROR}" )
print(f"DB_DELTA is : {DB_ERROR}" )
print(f"DB_STREAMREAD is : {DB_STREAMREAD}" )
print(f"DB_STREAMCHECKPOINT is : {DB_STREAMCHECKPOINT}" )
