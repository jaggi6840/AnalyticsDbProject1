# Databricks notebook source
# MAGIC %run "../set-up/schema"

# COMMAND ----------

df = spark.read.format("csv") \
           .option("INFERSCHEMA" , True) \
            .option("HEADER" , True) \
          .load(f"{DB_STREAMREAD}/sale1.txt").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC **STREAMING DONT WORK WITHOUT SCHEMA**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType
Schema = StructType([
                    StructField("Sale" , StringType(), True),
                    StructField("Sale_Count", StringType() , True)
                    ])

# COMMAND ----------

df = spark.readStream.format("csv") \
           .schema(Schema) \
           .option("header",True) \
           .load("/mnt/covidreportingdatalake6/streamread")
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### STREAD WRITE HAS OUPUT MODE as Append ,Update etc also syntax is different then normal write 
# MAGIC 1.make sure to write .start()

# COMMAND ----------

df.writeStream.format("parquet")\
               .option("checkpointLocation" ,f"{DB_STREAMCHECKPOINT}" ) \
                .outputMode("append") \
                .trigger(processingTime="1 minute") \
               .option("path" , "/mnt/covidreportingdatalake6/streamwrite").start()

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.format("parquet") \
            .option("HEADER" , True) \
            .load("/mnt/covidreportingdatalake6/streamwrite").show()
            # \ \
            #     .write.format("parquet") \
            # .saveAsTable("jaggiqqqqq")

# COMMAND ----------

# MAGIC %sql SELECT * FROM jaggiqqqqq
