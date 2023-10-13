# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='Analytics')

# COMMAND ----------

client_id= dbutils.secrets.get(scope='Analytics' , key = 'Analytics-Client-id')

# COMMAND ----------

tenant_id= dbutils.secrets.get(scope='Analytics' , key = 'Analytics-tenant-id')

# COMMAND ----------

client_secret= dbutils.secrets.get(scope='Analytics' , key = 'Analytics-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# client_id="645c735b-0a5d-4c34-88a0-1fdef53deb3c"
# tenant_id="fb21a52b-1494-4328-95b2-c74714343e12"
# client_secret="CGf8Q~q4eOAOvF~W80OD.U2r~Nm8DIF2y.tk0dm9"

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.analyticsdbhub1.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.analyticsdbhub1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.analyticsdbhub1.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.analyticsdbhub1.dfs.core.windows.net", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.analyticsdbhub1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@analyticsdbhub1.dfs.core.windows.net")
