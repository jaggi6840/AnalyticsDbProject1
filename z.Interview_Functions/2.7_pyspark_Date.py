# Databricks notebook source
df =spark.createDataFrame([(1000, 'login', '2023-06-16 01:02:15.34'),
(1000, 'login', '2023-06-16 02:02:15.34'),
(1000, 'login', '2023-06-16 03:02:15.34'),
(1000, 'logout', '2023-06-16 12:03:15.34'),
(1001, 'login', '2023-06-16 01:04:15.34'),
(1001, 'login', '2023-06-16 02:05:15.34'),
(1001, 'login', '2023-06-16 03:06:15.34'),
(1001, 'logout', '2023-06-16 12:07:15.34')],
["employee_id","entry_details","hire_date"])
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col,year,month,dayofmonth,date_add,dayofyear,dayofweek,datediff,current_date,minute

# COMMAND ----------

from pyspark.sql.functions import col,year,month,dayofmonth,date_add,dayofyear,dayofweek,datediff,current_date,minute,second,hour
df.withColumn("birth_year", year(col('hire_date'))) \
                 .withColumn("birth_day", dayofmonth(col('hire_date'))) \
                 .withColumn("dayofweek", dayofweek(col('hire_date'))) \
                 .withColumn("datediff", datediff(current_date(),(col('hire_date')))) \
                 .withColumn("birth_day", dayofmonth(col('hire_date'))) \
                 .withColumn("day_of_year", dayofyear(col('hire_date'))) \
                 .withColumn("birth_month", month(col('hire_date'))) \
                 .withColumn("15_day_after", date_add(col('hire_date'),15)) \
                 .withColumn("15_day_month", date_add(col('hire_date'),-15))\
                 .withColumn("minute", minute((col('hire_date')))) \
                 .withColumn("second", second((col('hire_date'))))\
                 .withColumn("hour", hour((col('hire_date')))).show()
