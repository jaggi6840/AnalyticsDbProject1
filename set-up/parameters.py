# Databricks notebook source
# MAGIC %md
# MAGIC ###### Creating Parameters for all the containers
# MAGIC

# COMMAND ----------

DB_RAW='/mnt/covidreportingdatalake6/raw'
DB_PROCESSED='/mnt/covidreportingdatalake6/processed'
DB_LOOKUP='/mnt/covidreportingdatalake6/lookup'
DB_ERROR='/mnt/covidreportingdatalake6/error'

DB_STREAMWRITE='/mnt/covidreportingdatalake6/streamwrite'
DB_STREAMREAD='/mnt/covidreportingdatalake6/streamread'
DB_STREAMCHECKPOINT='/mnt/covidreportingdatalake6/streamcheckpoint'
DB_DELTA='/mnt/covidreportingdatalake6/delta'


print(f"DB_Raw is : {DB_RAW}" )
print(f"DB_PROCESSED is : {DB_PROCESSED}" )
print(f"DB_LOOKUP is : {DB_LOOKUP}" )
print(f"DB_STREAMWRITE is : {DB_STREAMWRITE}" )
print(f"DB_STREAMREAD is : {DB_STREAMREAD}" )
print(f"DB_STREAMCHECKPOINT is : {DB_STREAMCHECKPOINT}" )
print(f"DB_ERROR is : {DB_ERROR}" )
print(f"DB_DELTA is : {DB_DELTA}" )
