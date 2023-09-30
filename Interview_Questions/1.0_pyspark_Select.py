# Databricks notebook source
# MAGIC %md 
# MAGIC ###### How to use Select Function 4 methods

# COMMAND ----------

employee_data = [(1000,"Mieeeel","Colombus","USA", 3125329880),
                 (2000,"Michael","Chicago","USA", 3125329880),
                 (4000,"Nancy","New York","USA", 7623434343)]
employee_schema = ["employee_id","name","country","phone"]               
df= spark.createDataFrame(employee_data, employee_schema)

# COMMAND ----------

df.select("employee_id",col('name'),df.name , df["name"]).show()
