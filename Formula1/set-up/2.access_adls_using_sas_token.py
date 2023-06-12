# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.formula1atharvadl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1atharvadl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1atharvadl.dfs.core.windows.net", "sp=rl&st=2023-04-15T23:08:28Z&se=2023-04-16T07:08:28Z&spr=https&sv=2021-12-02&sr=c&sig=nXxmhrPWVgn5n3XNEFH3pM9G0U%2Fn8E7TVXimCHNPDWU%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1atharvadl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1atharvadl.dfs.core.windows.net"))