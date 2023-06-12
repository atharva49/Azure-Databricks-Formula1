-- Databricks notebook source
-- MAGIC %run "../includes/configurations"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- DBTITLE 1, 
DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lon DOUBLE,
  alt INT,
  url STRING
)USING csv
OPTIONS (path "abfss://raw@formula1atharvadl.dfs.core.windows.net/circuits.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)USING csv
OPTIONS (path "abfss://raw@formula1atharvadl.dfs.core.windows.net/races.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING json
OPTIONS (path "abfss://raw@formula1atharvadl.dfs.core.windows.net/constructors.json", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT <forename STRING, surname STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path "abfss://raw@formula1atharvadl.dfs.core.windows.net/drivers.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
USING json
OPTIONS (path "abfss://raw@formula1atharvadl.dfs.core.windows.net/results.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId INT,
driverId INT,
stop STRING,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING json
OPTIONS (path "abfss://raw@formula1atharvadl.dfs.core.windows.net/pit_stops.json",multiLine true, header true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS(path "abfss://raw@formula1atharvadl.dfs.core.windows.net/lap_times",header true);

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING json
OPTIONS(path "abfss://raw@formula1atharvadl.dfs.core.windows.net/qualifying",header true, multiLine true);

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;