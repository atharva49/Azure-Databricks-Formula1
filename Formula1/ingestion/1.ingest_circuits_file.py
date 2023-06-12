# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest circuits.csv file
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

print(v_data_source)

# COMMAND ----------

# MAGIC %run "../includes/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, IntegerType, StructType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(), False),
                                     StructField("circuitRef",StringType(), True),
                                     StructField("name",StringType(), True),
                                     StructField("location",StringType(), True),
                                     StructField("country",StringType(), True),
                                     StructField("lat",DoubleType(), True),
                                     StructField("lng",DoubleType(), True),
                                     StructField("alt",IntegerType(), True),
                                     StructField("url",StringType(), True),
                                    ])

# COMMAND ----------

# display(dbutils.fs.ls(raw_folder_path))

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv",header=True,schema=circuits_schema)

# COMMAND ----------

# display(circuits_df)

# COMMAND ----------

# circuits_df.printSchema()

# COMMAND ----------

# circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecting only required columns
# MAGIC

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")
# display(circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,
                                          circuits_df.location,circuits_df.country.alias("race_country"),circuits_df.lat,
                                          circuits_df.lng,circuits_df.alt)
# display(circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(("circuitId"),("circuitRef"),("name"),("location"),("country"),("lat"),("lng"),("alt"))
# display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Renaming Columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
                                        .withColumnRenamed("circuitRef",'circuit_ref')\
                                        .withColumnRenamed("lat","latitude")\
                                        .withColumnRenamed("lng","longitude")\
                                        .withColumnRenamed("alt","altitide")\
                                        .withColumn("data_source",lit(v_data_source))\
                                        .withColumn("file_date",lit(v_file_date))

# display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Ingest new column for timestamp

# COMMAND ----------

final_df = add_ingestion_date(circuits_renamed_df)
# display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to datalake in parquet format

# COMMAND ----------


# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")
final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# display(dbutils.fs.ls(f"{processed_folder_path}/circuits"))
# df = spark.read.parquet(f"{processed_folder_path}/circuits")
# display(df)

# COMMAND ----------

# %sql
# DESCRIBE DATABASE EXTENDED f1_processed

# COMMAND ----------

# %sql
# SELECT *
# FROM f1_processed.circuits

# COMMAND ----------

# %sql
# DESC TABLE EXTENDED f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")