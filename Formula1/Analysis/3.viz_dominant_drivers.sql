-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on dominant F1 drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT    driver_name,
          count(1) AS total_races,
          sum(calculated_points) AS total_points,
          avg(calculated_points) AS avg_points,
          RANK() OVER(ORDER BY avg(calculated_points) DESC) AS driver_rank
FROM      f1_presentation.calculated_race_results
GROUP BY  driver_name
HAVING    total_races >=50
ORDER BY  avg_points DESC

-- COMMAND ----------

SELECT    race_year,
          driver_name,
          count(1) AS total_races,
          sum(calculated_points) AS total_points,
          avg(calculated_points) AS avg_points,
          RANK() OVER(ORDER BY avg(calculated_points) DESC) AS driver_rank
FROM      f1_presentation.calculated_race_results
WHERE     driver_name IN (
                          SELECT   driver_name
                          FROM     v_dominant_drivers
                          WHERE    driver_rank <=10
                          )
GROUP BY  driver_name,
          race_year
ORDER BY  avg_points DESC,
          race_year