# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date= dbutils.widgets.get("p_file_date") 

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
race_results_df = spark.read.format("delta").load("/mnt/formula1dl/presentation/race_results")\
                            .filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

race_results_list = df_column_to_list(race_results_df,"race_year")

# COMMAND ----------

race_year_list = []

for race_year in race_results_list:
    race_year_list.append(race_year)

# COMMAND ----------

from pyspark.sql.functions import sum, count, col, when

# COMMAND ----------

race_results_df = spark.read.format("delta").load("/mnt/formula1dl/presentation/race_results")\
                        .filter(col("race_year").isin(race_results_list))

# COMMAND ----------

# DBTITLE 0,o
team_standing_df = race_results_df\
                        .groupBy("race_year","team")\
                        .agg(sum("points").alias("total_points"),
                            count(when(col("position")==1, True)).alias("wins") )

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

team_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = team_standing_df.withColumn("rank",rank().over(team_rank_spec))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
# over_write_partition(final_df,'f1_presentation','constructor_standings','race_year')

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
folder_path = "/mnt/formula1dl/presentation"
merge_delta_data(final_df,"f1_presentation","constructor_standings",folder_path,merge_condition,"race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM  f1_presentation.constructor_standings