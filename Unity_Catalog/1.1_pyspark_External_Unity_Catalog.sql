-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ** CREATE EXTERNAL TABLES**
-- MAGIC 1. Bronze
-- MAGIC 1. Silver
-- MAGIC 1. Silver
-- MAGIC

-- COMMAND ----------

CREATE EXTERNAL LOCATION databrickscourseucext12_bronze
URL 'abfss://bronze@databrickscourseucext12.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-ext-storage-credential`);

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION databrickscourseucext12_bronze

-- COMMAND ----------

CREATE EXTERNAL LOCATION databrickscourseucext12_gold
URL 'abfss://gold@databrickscourseucext12.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-ext-storage-credential`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION databrickscourseucext12_silver
URL 'abfss://silver@databrickscourseucext12.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-ext-storage-credential`);
