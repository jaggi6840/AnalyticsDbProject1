-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Create Schemas and Catalogues**
-- MAGIC 1. Catalog formula1_dev(Without Managed Location)
-- MAGIC 1. Schemas - bronze, silver and Gold(With Managed Location)

-- COMMAND ----------

Create CATALOG IF NOT ExiSTS FORMULA_DEV;

-- COMMAND ----------

USE CATALOG FORMULA_DEV

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze

-- COMMAND ----------

SHOW CURRENT_CATALOG()

-- COMMAND ----------

CREATE SCHEMA IF  NOT  EXISTS  BRONZE
managed location 'abfss://bronze@databrickscourseucext12.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF  NOT  EXISTS  SILVER
managed location 'abfss://silver@databrickscourseucext12.dfs.core.windows.net/'

-- COMMAND ----------

CREATE SCHEMA IF  NOT  EXISTS  GOLD
managed location 'abfss://gold@databrickscourseucext12.dfs.core.windows.net/'

-- COMMAND ----------

SHOW SCHEMAS
