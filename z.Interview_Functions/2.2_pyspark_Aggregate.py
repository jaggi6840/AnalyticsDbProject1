# Databricks notebook source
# MAGIC %md 
# MAGIC **Aggregating Fields**

# COMMAND ----------

from pyspark.sql.functions import col,avg,max,min,first,last,mean,collect_list,collect_set,count
#Collect set will eliminate duplicate while collect_list gives everything

# COMMAND ----------

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Single Aggregate function**

# COMMAND ----------

df.select(avg(col('salary'))).show()
df.select(max(col('salary'))).show()
df.select(min(col('salary'))).show()
df.select(first(col('salary'))).show()
df.select(last(col('salary'))).show()
df.select(mean(col('salary'))).show()
df.select(collect_set(col('salary'))).show(truncate = False)
df.select(collect_list(col('salary'))).show(truncate = False)

# COMMAND ----------

# MAGIC %md 
# MAGIC **Multiple Aggregate function**

# COMMAND ----------

df.groupBy(col('department')).agg(max(col('salary')).alias('max'), \
                              (min(col('salary')).alias('min')), \
                              (avg(col('salary')).alias('avg'))).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC **Multiple Aggregate function with Where condition**

# COMMAND ----------

df.groupBy(col('department')).agg(max(col('salary')).alias('max'), \
                              (min(col('salary')).alias('min')), \
                              (avg(col('salary')).alias('avg')))\
                                .where(col('max')>4000).show()
