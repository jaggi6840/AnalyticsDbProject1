# Databricks notebook source
# MAGIC %md 
# MAGIC #### STREAD WRITE HAS OUPUT MODE as Append ,Update etc also syntax is different then normal write 
# MAGIC 1.make sure to write .start()

# COMMAND ----------

DB_RAW='/mnt/analyticsdbhub/raw'
DB_PROCESSED='/mnt/analyticsdbhub/processed'
DB_LOOKUP='/mnt/analyticsdbhub/lookup'
DB_ERROR='/mnt/analyticsdbhub/error'
DB_STREAMWRITE='/mnt/analyticsdbhub/streamwrite'
DB_STREAMREAD='/mnt/analyticsdbhub/streamread'
DB_STREAMCHECKPOINT='/mnt/analyticsdbhub/streamchekpoint'
DB_DELTA='/mnt/analyticsdbhub/delt'


# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
Sales_schema = StructType([
               StructField("Company" , StringType() , True),
               StructField("Sales"   , IntegerType(), True)
])

# COMMAND ----------

# df = spark.readStream.format("csv")\
#                 .schema(Sales_schema)\
#                  .option("header",True)\
#                 .load(f"{DB_STREAMREAD}")

# COMMAND ----------

# df2= df.writeStream.format("parquet")\
#                .option("checkpointLocation" ,"/mnt/analyticsdbhub/streamread" ) \
#                 .outputMode("append") \
#                 .trigger(processingTime="1 minute") \
#                .option("path" , "/mnt/analyticsdbhub/streamwrite").start()
# display(df2)               

# COMMAND ----------

spark.read.format("parquet").load("/mnt/analyticsdbhub/streamwrite").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC **AUTOLOADER**

# COMMAND ----------

# MAGIC %sql SHOW DATABASES

# COMMAND ----------

# MAGIC %sql USE stream

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
           .option("cloudFiles.format" , "csv") \
            .option("cloudFiles.schemaLocation" , "/mnt/analyticsdbhub/streamcheckpointschema/") \
           .load("/mnt/analyticsdbhub/streamread")\
            .writeStream\
            .option("checkpointLocation" ,"/mnt/analyticsdbhub/streamcheckpoint")\
            .option("path" , "/mnt/analyticsdbhub/streamwrite/auto") \
            .table("stream.sales")

# COMMAND ----------

#dbutils.fs.ls("/mnt/analyticsdbhub/streamcheckpoint")
dbutils.fs.ls("/mnt/analyticsdbhub/streamwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stream.sales
# MAGIC --truncate table stream.sales
