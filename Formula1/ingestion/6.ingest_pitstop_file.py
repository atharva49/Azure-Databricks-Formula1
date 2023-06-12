# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Pitstop Data

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

pit_stops_schema =  StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
                            ])

# COMMAND ----------

pit_stops_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/pit_stops.json",schema=pit_stops_schema,multiLine=True)
# display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("raceId","race_id")\
                                        .withColumnRenamed("driverId","driver_id")\
                                        .withColumn("data_source",lit(v_data_source))

add_ingestion_date(final_df)
# display(final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Method 2

# COMMAND ----------

# over_write_partition(final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df,"f1_processed","pit_stops","/mnt/formula1dl/processed",merge_condition,"race_id")

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# display(dbutils.fs.ls(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER By race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")