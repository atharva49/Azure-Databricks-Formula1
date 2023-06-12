-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1/presentation"

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE f1_presentation

-- COMMAND ----------

SELECT * FROM f1_presentation.race_results

-- COMMAND ----------

SELECT * FROM f1_presentation.driver_standings

-- COMMAND ----------

SELECT * FROM f1_presentation.constructor_standings