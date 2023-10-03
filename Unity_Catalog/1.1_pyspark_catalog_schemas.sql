-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Create Schemas and Catalogues**
-- MAGIC 1. Catalog formula1_dev(Without Managed Location)
-- MAGIC 1. Schemas - bronze, silver and Gold(With Managed Location)

-- COMMAND ----------

Create CATALOG IF NOT ExiSTS FORMULA_DEV1;

-- COMMAND ----------

USE CATALOG FORMULA_DEV1

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze

-- COMMAND ----------


