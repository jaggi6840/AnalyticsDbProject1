# Databricks notebook source
# MAGIC %md 
# MAGIC **INGEST CSV FILE**

# COMMAND ----------

dbutils.widgets.text("table_name" , "")
tbl_name = dbutils.widgets.get("table_name")

# COMMAND ----------

dbutils.widgets.text("schema_name" , "")
schema = dbutils.widgets.get("schema_name")

# COMMAND ----------

print(f"Moving data for {tbl_name} from RAW to BRONZE")

# COMMAND ----------

# MAGIC %run "../Set-Up/1.0_Schema"

# COMMAND ----------

# MAGIC %run "../Set-Up/1.0_Common_Functions"

# COMMAND ----------

# x= f"{tbl_name}"_schema

# COMMAND ----------

 copied_struct_schema = StructType(f"{tbl_name}"_schema.fields)


# COMMAND ----------

schema_name

# COMMAND ----------

countries_schema

# COMMAND ----------

# copied_struct_schema = StructType(countries_schema.fields)
# copied_struct_schema

# COMMAND ----------

# schema_name

# COMMAND ----------

# x = f"{tbl_name}_schema"
# x

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 1 - Reading CSV file from ADLS

# COMMAND ----------

# # Schema is stored in Set-Up/1.0_Schema"

# # Schema is not working schema({tbl_name}_schema)
# from pyspark.sql.functions import col
# Df_Input = spark.read.format("csv") \
#                      .schema(x)\
#                      .option("Header" , True)\
#                      .load(f"{DB_RAW}/{tbl_name}.csv")

# COMMAND ----------

# Schema is stored in Set-Up/1.0_Schema"

# Schema is not working schema({tbl_name}_schema)
from pyspark.sql.functions import col
Df_Input = spark.read.format("csv") \
                     .schema(schema)\
                     .option("Header" , True)\
                     .load(f"{DB_RAW}/{tbl_name}.csv")

# COMMAND ----------


spark.schema.help()

# COMMAND ----------

Df = add_date_file_name(Df_Input)

# COMMAND ----------

Df.write.format("parquet") \
                  .mode("Overwrite") \
                  .parquet(f"{DB_BRONZE}/{tbl_name}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3a - Create Database
# MAGIC 1. Create Database  

# COMMAND ----------

# MAGIC %sql CREATE  DATABASE IF NOT EXISTS BRONZE

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3b - Show Databases

# COMMAND ----------

# MAGIC %sql SHOW DATABASES

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3c - Change Database

# COMMAND ----------

# MAGIC %sql USE BRONZE

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 4 - Creating Temp and Global views

# COMMAND ----------

Df.createOrReplaceTempView(f"{tbl_name}")
Df.createOrReplaceGlobalTempView(f"{tbl_name}_g")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 5 - Storing data as Hive Managed Table

# COMMAND ----------

Df.write.format("parquet") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable(f"BRONZE.{tbl_name}_int")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 6 - Storing data as Hive EXTERNAL Table

# COMMAND ----------

Df.write.format("parquet") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_BRONZE}/{tbl_name}_ext" ) \
                  .saveAsTable(f"BRONZE.{tbl_name}_ext")

# COMMAND ----------

dbutils.notebook.exit(f"Notebook ran Successfully for {tbl_name}")
