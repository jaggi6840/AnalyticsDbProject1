# Databricks notebook source
# MAGIC %md 
# MAGIC **Transform CSV FILE**

# COMMAND ----------

dbutils.widgets.text("table_name" , "")
tbl_name = dbutils.widgets.get("table_name")

# COMMAND ----------

print(f"Creating Silver Table for {tbl_name}")

# COMMAND ----------

dbutils.widgets.text("table_name" , "")
tbl_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# MAGIC %run "../Set-Up/1.0_Schema"
# MAGIC

# COMMAND ----------

# MAGIC %run "../Set-Up/1.0_Common_Functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 1 - Reading Parquet file from Bronze

# COMMAND ----------

# Schema is stored in Set-Up/1.0_Schema"
Df_Input = spark.read.format("parquet") \
                     .load(f"{DB_BRONZE}/{tbl_name}")

# COMMAND ----------

from pyspark.sql.functions import col
DF1= Df_Input.dropDuplicates() \
             .drop('File_Path') \
              .dropDuplicates() \
                .dropna('all')
                  
display(DF1)

# COMMAND ----------

from pyspark.sql.functions import col,split
DF1= Df_Input.select("*") \
             .drop('File_Path') \
              .dropDuplicates() \
                  
display(DF1)

# COMMAND ----------

# MAGIC %sql CREATE DATABASE IF NOT EXISTS  SILVER;

# COMMAND ----------

# MAGIC %sql SHOW DATABASES

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE DATABASE SILVER

# COMMAND ----------

# MAGIC %sql SELECT CURRENT_DATABASE()

# COMMAND ----------

DF1.write.format("parquet") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_SILVER}/{tbl_name}_ext" ) \
                  .saveAsTable(f"SILVER.{tbl_name}_ext")

# COMMAND ----------

dbutils.notebook.exit(f"Notebook ran Successfully for {tbl_name}")
