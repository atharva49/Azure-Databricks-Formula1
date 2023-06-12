# Databricks notebook source
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

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType, DateType, TimestampType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                  StructField("year",IntegerType(),True),
                                  StructField("round",IntegerType(),True),
                                  StructField("circuitId",IntegerType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("date",DateType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("url",StringType(),True)
])

# COMMAND ----------

# display(dbutils.fs.ls(f"{raw_folder_path}"))

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv",header=True,schema = races_schema)
# display(races_df)

# COMMAND ----------

# races_df.printSchema()

# COMMAND ----------

races_selected_df = races_df.select(("raceId"),("year"),("round"),("circuitId"),("name"),("date"),("time"))
# display(races_selected_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("year","race_year")\
                                    .withColumnRenamed("circuitId","circuit_id")\
                                    .withColumn("data_source",lit(v_data_source))\
                                    .withColumn("file_date",lit(v_file_date))

# display(races_renamed_df)

# COMMAND ----------

circuits_new_col = add_ingestion_date(races_renamed_df)
# display(circuits_new_col)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat,lit

# COMMAND ----------

final_df = circuits_new_col\
.withColumn("race_timestamp",to_timestamp(concat('date',lit(' '),'time'),'yyyy-MM-dd HH:mm:ss'))

# display(races_final_df)

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/races")
final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# display(dbutils.fs.ls(f"{processed_folder_path}/races"))

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/races")))

# COMMAND ----------

# %sql
# SELECT *
# FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")