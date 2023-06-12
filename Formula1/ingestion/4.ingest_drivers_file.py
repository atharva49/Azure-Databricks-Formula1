# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file

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

from pyspark.sql.types import StringType, StructType, StructField, DateType, IntegerType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                StructField("surname", StringType(), True),
                    ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                            ])

# COMMAND ----------

# drivers_df.printSchema()

# COMMAND ----------

drivers_df = spark.read.json(path=f"{raw_folder_path}/{v_file_date}/drivers.json",schema=drivers_schema)
# display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col,concat, lit

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("driverRef","driver_ref")\
                                    .withColumn('name',concat(col("name.forename"),lit(' '),col("name.surname")))\
                                    .withColumn("data_source",lit(v_data_source))\
                                    .withColumn("file_date",lit(v_file_date))

drivers_with_columns_df = add_ingestion_date(drivers_with_columns_df)
# display(drivers_with_columns_df)

# COMMAND ----------

drivers_dropped_df = drivers_with_columns_df.drop('url')
# display(drivers_dropped_df)

# COMMAND ----------

# drivers_dropped_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
drivers_dropped_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# display(dbutils.fs.ls(f"{processed_folder_path}/drivers"))

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

# %sql
# SELECT *
# FROM f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")