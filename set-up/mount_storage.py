# Databricks notebook source
# MAGIC %md
# MAGIC ###### Mount the following data lake storage gen2 containers
# MAGIC   1. raw
# MAGIC   2. processed
# MAGIC   3. lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Set-up the configs
# MAGIC ##### Please update the following 
# MAGIC
# MAGIC   |Table|Content|
# MAGIC   |--|--|
# MAGIC   |01|application-id|
# MAGIC   |02|service-credential|
# MAGIC   |03|directory-id|
# MAGIC

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "78219f8f-eaeb-4d31-8be2-895d2f98dd9b",
           "fs.azure.account.oauth2.client.secret": "e3p8Q~UoEifnIyqwGAZ1_z-vCAN7vnEK917X_aw5",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/1a8a013c-05a4-4ae2-a231-c624645288c9/oauth2/token"}

# COMMAND ----------

 
 dbutils.fs.mount(
   source = "abfss://streamwrite@covidreportingdatalake6.dfs.core.windows.net/",
   mount_point = "/mnt/covidreportingdatalake6/streamwrite",
   extra_configs = configs)
 

# COMMAND ----------


 dbutils.fs.mount(
   source = "abfss://raw@covidreportingdatalake6.dfs.core.windows.net/",
   mount_point = "/mnt/covidreportingdatalake6/raw",
   extra_configs = configs)
 
dbutils.fs.mount(
   source = "abfss://processed@covidreportingdatalake6.dfs.core.windows.net/",
   mount_point = "/mnt/covidreportingdatalake6/processed",
   extra_configs = configs)
 
 dbutils.fs.mount(
   source = "abfss://lookup@covidreportingdatalake6.dfs.core.windows.net/",
   mount_point = "/mnt/covidreportingdatalake6/lookup",
   extra_configs = configs)
 
  dbutils.fs.mount(
   source = "abfss://error@covidreportingdatalake6.dfs.core.windows.net/",
   mount_point = "/mnt/covidreportingdatalake6/error",
   extra_configs = configs)
  
 dbutils.fs.mount(
   source = "abfss://streamread@covidreportingdatalake6.dfs.core.windows.net/",
   mount_point = "/mnt/covidreportingdatalake6/streamread",
   extra_configs = configs)
 
 dbutils.fs.mount(
   source = "abfss://streamwrite@covidreportingdatalake6.dfs.core.windows.net/",
   mount_point = "/mnt/covidreportingdatalake6/streamwrite",
   extra_configs = configs)
 
 dbutils.fs.mount(
   source = "abfss://streamcheckpoint@covidreportingdatalake6.dfs.core.windows.net/",
   mount_point = "/mnt/covidreportingdatalake6/streamcheckpoint",
   extra_configs = configs)
  dbutils.fs.mount(
   source = "abfss://delta@covidreportingdatalake6.dfs.core.windows.net/",
   mount_point = "/mnt/covidreportingdatalake6/delta",
   extra_configs = configs)


# COMMAND ----------

dbutils.fs.mount(
source = "abfss://delta@covidreportingdatalake6.dfs.core.windows.net/",
    mount_point = "/mnt/covidreportingdatalake6/delta",
    extra_configs = configs)

# COMMAND ----------

DB_RAW='/mnt/covidreportingdatalake6/raw'
DB_PROCESSED='/mnt/covidreportingdatalake6/processed'
DB_LOOKUP='/mnt/covidreportingdatalake6/lookup'
DB_ERROR='/mnt/covidreportingdatalake6/error'
DB_STREAMWRITE='/mnt/covidreportingdatalake6/streamwrite'
DB_STREAMREAD='/mnt/covidreportingdatalake6/streamread'
DB_STREAMCHECKPOINT='/mnt/covidreportingdatalake6/streamcheckpoint'
DB_DELTA='/mnt/covidreportingdatalake6/delt'


print(f"DB_Raw is : {DB_RAW}" )
print(f"DB_PROCESSED is : {DB_PROCESSED}" )
print(f"DB_LOOKUP is : {DB_LOOKUP}" )
print(f"DB_ERROR is : {DB_ERROR}" )
print(f"DB_STREAMWRITE is : {DB_STREAMWRITE}" )
print(f"DB_ERROR1 is : {DB_ERROR}" )
print(f"DB_DELTA is : {DB_ERROR}" )
print(f"DB_STREAMREAD is : {DB_STREAMREAD}" )
print(f"DB_STREAMCHECKPOINT is : {DB_STREAMCHECKPOINT}" )


# COMMAND ----------

# MAGIC %md 
# MAGIC Unmount a mount point
# MAGIC

# COMMAND ----------

#  dbutils.fs.unmount("/mnt/covidreportingdatalake6/error")

# COMMAND ----------

# dbutils.fs.ls("/mnt/covidreportingdatalake6")

# COMMAND ----------

# print(f"{DB_ERROR}")
