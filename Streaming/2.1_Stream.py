# Databricks notebook source
# MAGIC %md 
# MAGIC #### STREAD WRITE HAS OUPUT MODE as Append ,Update etc also syntax is different then normal write 
# MAGIC 1.make sure to write .start()

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

DB_RAW='/mnt/analyticsdbhub/raw'
DB_PROCESSED='/mnt/analyticsdbhub/processed'
DB_LOOKUP='/mnt/analyticsdbhub/lookup'
DB_ERROR='/mnt/analyticsdbhub/error'
DB_STREAMWRITE='/mnt/analyticsdbhub/streamwrite'
DB_STREAMREAD='/mnt/analyticsdbhub/streamread'
DB_STREAMCHECKPOINT='/mnt/analyticsdbhub/streamcheckpoint'
DB_DELTA='/mnt/analyticsdbhub/delt'


# COMMAND ----------

df.printSchema

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
country_schema = StructType([
         StructField("COUNTRY_ID" , IntegerType(),True),
         StructField("NAME" , StringType(),True),
         StructField("NATIONALITY" , StringType(),True),
         StructField("COUNTRY_CODE" , StringType(),True),
         StructField("ISO_ALPHA2" , StringType(),True),
         StructField("CAPITAL" , StringType(),True),
         StructField("POPULATION" , IntegerType(),True),
         StructField("AREA_KM2" , DoubleType(),True),
         StructField("REGION_ID" , IntegerType(),True),
         StructField("SUB_REGION_ID" , IntegerType(),True),
         StructField("INTERMEDIATE_REGION_ID" , IntegerType(),True),
         StructField("ORGANIZATION_REGION_ID" , IntegerType(),True)
])

# COMMAND ----------

fulldf = spark.read.format("csv")\
                .schema(country_schema)\
                 .option("header",True)\
                .load(f"{DB_STREAMREAD}/countries.csv")
display(fulldf)                

# COMMAND ----------

fulldf.write.format("delta")\
         .mode("Overwrite")\
         .save(f"{DB_PROCESSED}/countries/full")
df= fulldf.limit(10)
df.write.format("delta")\
         .mode("Overwrite")\
         .save(f"{DB_PROCESSED}/countries/small")


# COMMAND ----------

# MAGIC %sql INSERT INTO delta.small 
# MAGIC SELECT * FROM delta.full limit 10 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df.withColumn("inserted" , current_timestamp())

# COMMAND ----------

# MAGIC %sql 
# MAGIC --create table delta.full using delta location '/mnt/analyticsdbhub/processed/countries/full'
# MAGIC create table delta.small using delta location '/mnt/analyticsdbhub/processed/countries/small'

# COMMAND ----------

df = spark.readStream.format("delta") \
           .load("/mnt/analyticsdbhub/processed/countries/small")
display(df)

# COMMAND ----------

DB_STREAMWRITE="/mnt/analyticsdbhub/streamwrite"
df3= df.writeStream.format("parquet")\
               .option("checkpointLocation" ,f"{DB_STREAMCHECKPOINT}" ) \
               .option("path" , "/mnt/analyticsdbhub/streamwrite/countries")\
                   .trigger(once=True).start()

# COMMAND ----------

df =  spark.readStream.format("csv") \
           .schema(country_schema) \
           .option("header",True) \
           .load("f{DB_PROCESSED}/countries/small")
display(df)

# COMMAND ----------

streamQuery.recentProgress
