-- Databricks notebook source
-- MAGIC %run "../includes/configurations"

-- COMMAND ----------

-- CREATE DATABASE demo;
CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DESCRIBE demo.race_results_python

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- %python
-- race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_python_ext_py")

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_python_ext_py

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_race_results
AS
SELECT *
FROM  demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW gv_race_results
AS
SELECT *
FROM  demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
FROM  demo.race_results_python
WHERE race_year = 2000