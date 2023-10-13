# Databricks notebook source
v_result = dbutils.notebook.run("../Ingestion/1.0_Ingest_CSV_Files" , 0 , {"table_name" :"countries"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("../Ingestion/1.0_Ingest_CSV_Files" , 0 , {"table_name" :"country_regions"})

# COMMAND ----------

v_result
