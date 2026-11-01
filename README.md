# End-to-End ETL Project

This repository contains a Databricks notebook-based ETL pipeline that moves data through bronze, silver, and gold layers and then builds a fact table for downstream analytics.

## Repository contents

* `parameters` - defines the dataset inputs used by the job.
* `Bronze_Layer` - ingests source parquet files into the bronze layer with Auto Loader.
* `Silver_Customers` - transforms customer data into the silver layer.
* `Silver_Orders` - transforms order data into the silver layer.
* `Silver_Products` - transforms product data into the silver layer.
* `Silver_Regions` - transforms region data into the silver layer.
* `Gold_Customers` - builds customer-focused gold outputs.
* `Gold_Products` - builds product-focused gold outputs.
* `Fact Orders` - creates the final fact table.
* `job_config_end_to_end_pipeline.json` - exported Databricks Job configuration for the pipeline.

## Job orchestration

The Databricks Job `End-To-End-Pipeline` runs the notebooks in this order:

1. `parameters`
2. `Bronze_Layer` (fan-out using input datasets)
3. `Silver_Customers`, `Silver_Orders`, `Silver_Products`
4. `Gold_Customers`, `Gold_Products`
5. `Fact Orders`

## Prerequisites

* Access to the Databricks workspace and Unity Catalog objects used by the notebooks.
* Access to the external storage locations referenced in the notebook code.
* A configured Databricks Job or another orchestrator to run the notebooks in dependency order.

## Notes

* Do not commit secrets, tokens, or passwords to this repository.
* External data in cloud storage is not stored in GitHub; document or parameterize those paths separately.
* The job configuration file in this repo should be updated whenever the Databricks Job changes.
