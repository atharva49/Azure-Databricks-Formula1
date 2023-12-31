# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/constructors.json",schema=constructors_schema)
# display(constructor_df)

# COMMAND ----------

# constructor_df.printSchema()

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')
# display(constructor_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                                .withColumnRenamed("constructorRef","constructor_ref")\
                                                .withColumn("data_source",lit(v_data_source))\
                                                .withColumn("file_date",lit(v_file_date))

constructor_final_df = add_ingestion_date(constructor_final_df)
# display(constructor_final_df)

# COMMAND ----------

# constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# %sql
# SELECT *
# FROM f1_processed.constructors

# COMMAND ----------

# display(dbutils.fs.ls(f"{processed_folder_path}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("Success")