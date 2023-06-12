# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest Lap times

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
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, FloatType, StructField, StructType

# COMMAND ----------

lap_time_schema =  StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
                            ])

# COMMAND ----------

lap_times_df = spark.read.csv(path=f"{raw_folder_path}/{v_file_date}/lap_times",schema=lap_time_schema)
# lap_times_df = spark.read.csv(path=f"{raw_folder_path}/lap_times/lap_times*.csv",schema=lap_time_schema)

# display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("raceId","race_id")\
                                        .withColumnRenamed("driverId","driver_id")\
                                        .withColumn("data_source",lit(v_data_source))
                                        
add_ingestion_date(final_df)
# display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

# over_write_partition(final_df,'f1_processed','lap_times','race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df,"f1_processed","lap_times","/mnt/formula1dl/processed",merge_condition,"race_id")

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# display(dbutils.fs.ls(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER By race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")