# Azure-Databricks-Formula1

A Pipeline is created which executes another two interdependent pipelines
![Main Pipeline](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/d3ea5284-92fc-47cb-aadb-111f51b753c6)

Data Ingestion pipeline initiated. Pipeline checks if folder for required date is present and initiates the pipline. True conditin exectues all the ingestion databricks notebooks.
![Ingestion Pipeline 1](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/81216904-0d79-4047-b85b-4b62a7f84525)

Databricks ingestion notebooks
![Ingestion Pipeline True condition](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/5be603ad-a9f7-4398-b639-e9adfa98c04a)

Transformation pipeline initiated. Pipeline checks if folder for required date is present and initiates the pipline. True conditin exectues all the transformation databricks notebooks.
![Transformation Pipeline 1](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/2df54979-62b6-4a63-9d72-2f0a4a0f9bc3)

Databricks Transformtion notebooks
![Transformation Pipeline True condition](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/ccd56717-156a-4605-a3bb-2270fde745f8)

Trigger created to automate ingestion using tumblin window. Trigger is executed every Sunday at 10Pm
![Trigger](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/56649de7-9999-427e-a571-7169c54381f6)

Databricks Workspace
![Databricks Workspace 1](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/bf09b37a-8784-4e6c-b1e3-05a794764f98)
![Databricks Ingestion](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/1dda6ec3-2c4a-4e54-8753-c96bb4433a8f)

Hive Metastore
![Databricks Transformation](https://github.com/atharva49/Azure-Databricks-Formula1/assets/56105570/45b10b21-f601-4b6d-97af-d13aed2aa410)
