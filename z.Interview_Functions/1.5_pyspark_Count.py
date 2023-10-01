# Databricks notebook source
# MAGIC %md 
# MAGIC ###### How to use explode Function

# COMMAND ----------

data = [(1,'a',["PYTHON","jAVA"])]
schema = ["employee_id","name","SUB"]   
df= spark.createDataFrame(data, schema)

# COMMAND ----------

df.count()

# COMMAND ----------

df.limit(1)
