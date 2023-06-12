# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.formula1atharvadl.dfs.core.windows.net",
    "uiOUDuwcom+TQr9xIfXVffjJ2WeniFnqeHL9G3kSDAEp+bSPzRniPezfPRxUqjJfF8rfXMUGslnl+AStufkXRQ=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1atharvadl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1atharvadl.dfs.core.windows.net"))