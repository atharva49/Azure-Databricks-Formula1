# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Qualifying

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, FloatType, StructField, StructType

# COMMAND ----------


qualifying_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/qualifying",multiLine=True)
# display(qualifying_df)

# COMMAND ----------

# qualifying_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyingId","qualifying_id")\
                        .withColumnRenamed("raceId","race_id")\
                        .withColumnRenamed("driverId","driver_id")\
                        .withColumnRenamed("constructorId","constructor_id")\
                        .withColumn("data_source",lit(v_data_source))

add_ingestion_date(final_df)
# display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

# over_write_partition(final_df,'f1_processed','qualifying','race_id')

# COMMAND ----------

merge_condition = "tgt.qualifyId = src.qualifyId"
merge_delta_data(final_df,"f1_processed","qualifying","/mnt/formula1dl/processed",merge_condition,"race_id")

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# display(dbutils.fs.ls(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER By race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")