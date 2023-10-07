# Databricks notebook source
# MAGIC %md
# MAGIC ###### Mount the following data lake storage gen2 containers
# MAGIC   1. raw
# MAGIC   2. processed
# MAGIC   3. lookup

# COMMAND ----------

# client_id="dac1c3fa-2139-4ff1-b61e-bc2d6a087383"
# tenant_id="fb21a52b-1494-4328-95b2-c74714343e12"
# client_secret="FxV8Q~-2.X8yQf.8VSfbqn_UI4ScjIO3iF33ObZ7"

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='Analytics-Cloud-Secret-Scope')

# COMMAND ----------

client_id = dbutils.secrets.get(scope='Analytics-Cloud-Secret-Scope' ,key='Cloud-Analytics-Client-Id-Dev')
tenant_id = dbutils.secrets.get(scope='Analytics-Cloud-Secret-Scope' ,key='Cloud-Analytics-Tenant-Id-Dev')
client_secret = dbutils.secrets.get(scope='Analytics-Cloud-Secret-Scope' ,key='Cloud-Analytics-Client-Secret-Dev')

# COMMAND ----------

#client_id="dac1c3fa-2139-4ff1-b61e-bc2d6a087383"
#tenant_id="fb21a52b-1494-4328-95b2-c74714343e12"
#client_secret="FxV8Q~-2.X8yQf.8VSfbqn_UI4ScjIO3iF33ObZ7"

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.analyticsdbhub.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.analyticsdbhub.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.analyticsdbhub.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.analyticsdbhub.dfs.core.windows.net", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.analyticsdbhub.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.ls("abfss://demo@analyticsdbhub.dfs.core.windows.net")

# COMMAND ----------


# dbutils.fs.mount(
#    source = "abfss://raw@analyticsdbhub.dfs.core.windows.net",
#    mount_point = "/mnt/analyticsdbhub /raw",
#    extra_configs = configs)

# COMMAND ----------

# dbutils.fs.unmount('/mnt/analyticsdbhub/raw')
# dbutils.fs.unmount('/mnt/analyticsdbhub/demo')
# dbutils.fs.unmount('/mnt/analyticsdbhub/processed')
# dbutils.fs.unmount('/mnt/analyticsdbhub/lookup')
# dbutils.fs.unmount('/mnt/analyticsdbhub/presentation')
# dbutils.fs.unmount('/mnt/analyticsdbhub/checkpoint')
# dbutils.fs.unmount('/mnt/analyticsdbhub/delta')

# COMMAND ----------


dbutils.fs.mount(
   source = "abfss://raw@analyticsdbhub.dfs.core.windows.net",
   mount_point = "/mnt/analyticsdbhub/raw",
   extra_configs = configs)

dbutils.fs.mount(
   source = "abfss://demo@analyticsdbhub.dfs.core.windows.net",
   mount_point = "/mnt/analyticsdbhub/demo",
   extra_configs = configs)

 
dbutils.fs.mount(
   source = "abfss://processed@analyticsdbhub.dfs.core.windows.net/",
   mount_point = "/mnt/analyticsdbhub/processed",
   extra_configs = configs)
 
dbutils.fs.mount(
   source = "abfss://lookup@analyticsdbhub.dfs.core.windows.net/",
   mount_point = "/mnt/analyticsdbhub/lookup",
   extra_configs = configs)

dbutils.fs.mount(
   source = "abfss://presentation@analyticsdbhub.dfs.core.windows.net/",
   mount_point = "/mnt/analyticsdbhub/presentation",
   extra_configs = configs)
 
  
dbutils.fs.mount(
   source = "abfss://checkpoint@analyticsdbhub.dfs.core.windows.net/",
   mount_point = "/mnt/analyticsdbhub/checkpoint",
   extra_configs = configs)
 
dbutils.fs.mount(
source = "abfss://delta@analyticsdbhub.dfs.core.windows.net/",
    mount_point = "/mnt/analyticsdbhub/delta",
    extra_configs = configs) 

# COMMAND ----------

DB_RAW='/mnt/analyticsdbhub/raw'
DB_PROCESSED='/mnt/analyticsdbhub/processed'
DB_LOOKUP='/mnt/analyticsdbhub/lookup'
DB_ERROR='/mnt/analyticsdbhub/error'
DB_STREAMWRITE='/mnt/analyticsdbhub/streamwrite'
DB_STREAMREAD='/mnt/analyticsdbhub/streamread'
DB_STREAMCHECKPOINT='/mnt/analyticsdbhub/streamcheckpoint'
DB_DELTA='/mnt/analyticsdbhub/delt'


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

# COMMAND ----------

dbutils.fs.ls('/mnt/analyticsdbhub/raw')

# COMMAND ----------

dbutils.fs.cp('/mnt/analyticsdbhub/raw/countries.csv', '/mnt/analyticsdbhub/raw/countries_bkp.csv')

# COMMAND ----------

dbutils.fs.head('/mnt/analyticsdbhub/raw/countries.csv',25)

# COMMAND ----------

dbutils.fs.mkdirs('/mnt/analyticsdbhub/raw/jaggi')

# COMMAND ----------

# Mv is not working
dbutils.fs.mv('/mnt/analyticsdbhub/raw/countries.csv' , '/mnt/analyticsdbhub/raw/jaggi')


# COMMAND ----------

#Without True files will not ber overwritten
dbutils.fs.put("/mnt/analyticsdbhub/raw/hello_db.txt", "Hello1, Databricks!",True)

# COMMAND ----------

dbutils.fs.refreshMounts()


# COMMAND ----------

dbutils.fs.rm("/mnt/analyticsdbhub/raw/hello_db.txt")


# COMMAND ----------

#updateMount command (dbutils.fs.updateMount)
dbutils.fs.updateMount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)


# COMMAND ----------

#exit command (dbutils.notebook.exit)
dbutils.notebook.exit("Exiting from My Other Notebook")


# COMMAND ----------

#get command (dbutils.secrets.get)

dbutils.secrets.get(scope="my-scope", key="my-key")


# COMMAND ----------

dbutils.secrets.listScopes()


# COMMAND ----------

dbutils.widgets.remove('fruits_combobox')

