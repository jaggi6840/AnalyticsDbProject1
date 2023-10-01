# Databricks notebook source
df =spark.createDataFrame([(1000, 'login', '2023-06-16 01:00:15.34'),
(1000, 'login', '2023-06-16 02:00:15.34'),
(1000, 'login', '2023-06-16 03:00:15.34'),
(1000, 'logout', '2023-06-16 12:00:15.34'),
(1001, 'login', '2023-06-16 01:00:15.34'),
(1001, 'login', '2023-06-16 02:00:15.34'),
(1001, 'login', '2023-06-16 03:00:15.34'),
(1001, 'logout', '2023-06-16 12:00:15.34')],
["employee_id","entry_details","time_stamp_detail"])
df.show(truncate=False)

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
from pyspark.sql.functions import date_format
df1 = df.withColumn("year", date_format(col("time_stamp_detail"),'yyyy'))\
.withColumn("month",date_format(col("time_stamp_detail"),'MM'))\
.withColumn("day",date_format(col("time_stamp_detail"),'dd'))\
.withColumn("hour",date_format(col("time_stamp_detail"),'HH'))\
.withColumn("minutes",date_format(col("time_stamp_detail"),'mm'))\
.withColumn("seconds", date_format(col("time_stamp_detail"),'ss'))\
.withColumn("SSS", date_format(col("time_stamp_detail"),'SSS'))\
.withColumn("time",date_format(col("time_stamp_detail"),"HH:mm:ss:.SSS"))\
.withColumn("date", date_format(col("time_stamp_detail"),"YYYY-MM-DD")).show()
