# Databricks notebook source
# MAGIC %md 
# MAGIC ###### How to use explode Function

# COMMAND ----------

data = [(1,'a',["PYTHON","jAVA"])]
schema = ["employee_id","name","SUB"]   
df= spark.createDataFrame(data, schema)

# COMMAND ----------

from pyspark.sql.functions import col, explode
df.withColumn("subject" , explode(col('SUB'))).drop(col('SUB')).show()
