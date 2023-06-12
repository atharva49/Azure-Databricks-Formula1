# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Results data

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

results_schema = StructType(fields=[StructField("resultId",IntegerType(), False),
                                    StructField("raceId",IntegerType(), False),
                                    StructField("driverId",IntegerType(), False),
                                    StructField("constructorId",IntegerType(), False),
                                    StructField("number",IntegerType(), True),
                                    StructField("grid",IntegerType(), True),
                                    StructField("position",IntegerType(), True),
                                    StructField("positionText",StringType(), True),
                                    StructField("positionOrder",IntegerType(), True),
                                    StructField("points",FloatType(), True),
                                    StructField("laps",IntegerType(), True),
                                    StructField("time",StringType(), True),
                                    StructField("milliseconds",IntegerType(), True),
                                    StructField("fastestLap",IntegerType(), True),
                                    StructField("rank",IntegerType(), True),
                                    StructField("fastestLapTime",StringType(), True),
                                    StructField("fastestLapSpeed",StringType(), True),
                                    StructField("statusId",IntegerType(), True)
                            ])

# COMMAND ----------

results_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/results.json",schema=results_schema)
# display(results_df)

# COMMAND ----------

# results_df.printSchema()

# COMMAND ----------

results_dropped_df =  results_df.drop('statusId')
# results_dropped_df.printSchema()

# COMMAND ----------

#  results_dropped_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_with_columns = results_dropped_df.withColumnRenamed("resultId","result_id")\
                                        .withColumnRenamed("raceId","race_id")\
                                        .withColumnRenamed("driverId","driver_id")\
                                        .withColumnRenamed("constructorId","constructor_id")\
                                        .withColumnRenamed("positionText","position_text")\
                                        .withColumnRenamed("positionOrder","position_order")\
                                        .withColumnRenamed("fastestLap","fastest_lap")\
                                        .withColumnRenamed("fastestLapTimeltId","fastest_lap_time")\
                                        .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                        .withColumn("data_source",lit(v_data_source))\
                                        .withColumn("file_date",lit(v_file_date))

results_final_df = add_ingestion_date(results_with_columns) 
# display(results_with_columns)

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# # #results_with_columns.write.mode("append").format("parquet").option("path",f"{processed_folder_path}/results").saveAsTable("f1_processed.results")
# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

# over_write_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df,"f1_processed","results","/mnt/formula1dl/processed",merge_condition,"race_id")

# COMMAND ----------

# %sql
# DESC TABLE EXTENDED f1_processed.results

# COMMAND ----------

# display(dbutils.fs.ls(f"{processed_folder_path}/results"))

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# %sql
# SELECT *
# FROM f1_processed.results


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER By race_id DESC

# COMMAND ----------

dbutils.notebook.exit("Success")