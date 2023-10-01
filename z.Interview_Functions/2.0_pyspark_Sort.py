# Databricks notebook source
# MAGIC %md 
# MAGIC **Sort and OrderBy Fields**
# MAGIC 1. Sort and Orderby is same in pyspark

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

simpleData = [(6991,"6/9/2014 18:26","engagement","home_page","United States","iphone 5"), \
(18851,"8/29/2014 13:18","signup_flow","enter_info","Russia","asus chromebook"), \
(18851,"8/21/2014 13:18","signup_flow","enter_info","Russia","asus chromebook"), \
(14998,"7/1/2014 12:47","engagement","login","France","hp pavilion desktop"), \
(8186,"5/23/2014 10:44","engagement","home_page","Italy","macbook pro"), \
(9626,"7/31/2014 17:15","engagement","login","Russia","nexus 7"), \
(251,"8/6/2014 15:24",None,"login","Argentina","macbook air"), \
(238,"7/16/2014 14:28","engagement","login","Venezuela","samsung galaxy note"), \
(251,"8/2/2014 10:47",None,"login","Argentina","macbook air"), \
(108,"7/21/2014 17:34","engagement","login","Mexico","hp pavilion desktop"), \
(None,"6/25/2014 11:10","engagement","like_message","Argentina","macbook pro"), \
(16170,"8/23/2014 18:53",None,"like_message","Argentina","macbook pro"), \
(12103,"6/19/2014 19:45","engagement","home_page","Argentina","macbook pro"), \
(None,"6/25/2014 11:08","engagement","like_message","Argentina","macbook pro"), \
(16170,"8/25/2014 13:32","engagement","home_page","Argentina","macbook pro"), \
(12103,"6/25/2014 11:07","engagement","login","Argentina","macbook pro"), \
(12103,"6/19/2014 19:47","engagement","home_page","Argentina","macbook pro"), \
(None,None,None,None,None,None)
]
columns= ["user_id","occurred_at","event_type","event_name","location","device"]
df=spark.createDataFrame(simpleData,columns)

# COMMAND ----------

# MAGIC %md
# MAGIC **Srot on one Field**

# COMMAND ----------

df1 = df.sort(col('user_id'))

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Srot on multiple Field**

# COMMAND ----------

df2 = df.sort(col('user_id').desc() , col('occurred_at'))

# COMMAND ----------

display(df2)

# COMMAND ----------

df3= df.orderBy(col('user_id').desc() , col('occurred_at').asc())

# COMMAND ----------

display(df3)
