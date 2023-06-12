# Azure-Databricks Project

# Azure Databricks

•	Built a solution architecture for a data engineering solution using Azure Databricks, Azure Data Lake Gen2, Azure Data Factory, and Power BI.

•	Created and used Azure Databricks service and the architecture of Databricks within Azure.

•	Worked with Databricks notebooks and used Databricks utilities, magic commands, etc.

•	Passed parameters between notebooks as well as created notebook workflows.

•	Created, configured, and monitored Databricks clusters, cluster pools, and jobs.

•	Mounted Azure Storage in Databricks using secrets stored in Azure Key Vault.

•	Worked with Databricks Tables, Databricks File System (DBFS), etc.

•	Used Delta Lake to implement a solution using Lakehouse architecture.

•	Created dashboards to visualize the outputs.

•	Connected to the Azure Databricks tables from PowerBI.

# Spark (Only PySpark and SQL)

•	Spark architecture, Data Sources API and Dataframe API.

•	PySpark - Ingested CSV, simple and complex JSON files into the data lake as parquet files/ tables.

•	PySpark - Transformations such as Filter, Join, Simple Aggregations, GroupBy, Window functions etc.

•	PySpark - Created local and temporary views.

•	Spark SQL - Created databases, tables and views.

•	Spark SQL - Transformations such as Filter, Join, Simple Aggregations, GroupBy, Window functions etc.

•	Spark SQL - Created local and temporary views.

•	Implemented full refresh and incremental load patterns using partitions.

# Delta Lake
•	Performed Read, Write, Update, Delete, and Merge to delta lake using both PySpark as well as SQL.

•	History, Time Travel, and Vacuum.

•	Converted Parquet files to Delta files.

•	Implemented incremental load pattern using delta lake.

# Azure Data Factory

•	Created pipelines to execute Databricks notebooks.

•	Designed robust pipelines to deal with unexpected scenarios such as missing files.

•	Created dependencies between activities as well as pipelines.

•	Scheduled the pipelines using data factory triggers to execute at regular intervals.

•	Monitored the triggers/ pipelines to check for errors/ outputs.


# Screenshots


# A Pipeline is created which executes another two interdependent pipelines

![Main Pipeline](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/d3ea5284-92fc-47cb-aadb-111f51b753c6)

# Data Ingestion pipeline initiated. Pipeline checks if folder for required date is present and initiates the pipline. True conditin exectues all the ingestion databricks notebooks.

![Ingestion Pipeline 1](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/81216904-0d79-4047-b85b-4b62a7f84525)

# Databricks ingestion notebooks

![Ingestion Pipeline True condition](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/5be603ad-a9f7-4398-b639-e9adfa98c04a)

# Transformation pipeline initiated. Pipeline checks if folder for required date is present and initiates the pipline. True conditin exectues all the transformation databricks notebooks.

![Transformation Pipeline 1](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/2df54979-62b6-4a63-9d72-2f0a4a0f9bc3)

# Databricks Transformtion notebooks

![Transformation Pipeline True condition](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/ccd56717-156a-4605-a3bb-2270fde745f8)

# Trigger created to automate ingestion using tumblin window. Trigger is executed every Sunday at 10Pm
![Trigger](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/56649de7-9999-427e-a571-7169c54381f6)

Databricks Workspace

![Databricks Workspace 1](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/bf09b37a-8784-4e6c-b1e3-05a794764f98)
![Databricks Ingestion](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/1dda6ec3-2c4a-4e54-8753-c96bb4433a8f)
![Databricks Transformation](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/45b10b21-f601-4b6d-97af-d13aed2aa410)

# Hive Metastore

![Hive Metastore](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/eeab9e81-c69c-4f9e-b17b-1a44ac09b468)
