-- Databricks notebook source
SELECT * 
FROM  f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT    team_name,
          count(1) AS total_races,
          sum(calculated_points) AS total_points,
          sum(calculated_points)/count(1) AS avg_points
FROM      f1_presentation.calculated_race_results
GROUP BY  team_name
HAVING    total_races >100
ORDER BY  avg_points DESC

-- COMMAND ----------

SELECT    team_name,
          count(1) AS total_races,
          sum(calculated_points) AS total_points,
          sum(calculated_points)/count(1) AS avg_points
FROM      f1_presentation.calculated_race_results
WHERE     race_year BETWEEN 2010 AND 2020
GROUP BY  team_name
HAVING    total_races > 100
ORDER BY  avg_points DESC

-- COMMAND ----------

SELECT    team_name,
          count(1) AS total_races,
          sum(calculated_points) AS total_points,
          sum(calculated_points)/count(1) AS avg_points
FROM      f1_presentation.calculated_race_results
WHERE     race_year BETWEEN 2001 AND 2010
GROUP BY  team_name
HAVING    total_races > 100
ORDER BY  avg_points DESC