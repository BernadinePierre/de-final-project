# Crigglestone Data Pipeline

**Read this document carefully – it describes the architecture, components, and usage of the pipeline.**

## Objective

The Crigglestone Data Pipeline is designed to ingest, transform, and load data into a PostgreSQL data warehouse hosted on AWS. It demonstrates key data engineering practices including ETL automation, star schema modelling, cloud infrastructure, monitoring, and best practices in Python development.

By the end of the pipeline setup, you will have:

* Ingested raw operational data from a PostgreSQL source into an immutable S3 data lake.
* Transformed this data into dimension and fact tables aligned with a star schema.
* Loaded transformed data into a PostgreSQL data warehouse for analytics.
* Exported table previews to S3 for verification and lightweight reporting.
* Demonstrated monitoring and logging via AWS CloudWatch.

The solution showcases knowledge of Python, SQL, AWS services, and data engineering workflows.

---

## Overview

The pipeline consists of **three AWS Lambda functions**:

1. **Ingest Lambda**
   Extracts updated data from a source PostgreSQL database and stores it as CSV files in an ingestion S3 bucket.

2. **Transform Lambda**
   Reads CSVs from the ingest bucket, transforms them into dimensional and fact tables, and writes them as Parquet files into a processed S3 bucket.

3. **Load Lambda**
   Loads Parquet files into a PostgreSQL data warehouse and exports table previews to another S3 bucket.

This pipeline enables a robust ETL workflow, supporting analytics on business operations data.

---

## Prerequisites

### AWS

* AWS Lambda
* Amazon S3
* AWS Secrets Manager
* Amazon RDS (PostgreSQL)

### Python Libraries (included in deployment packages)

* `awswrangler`
* `pg8000`
* `boto3`
* `pandas`
* `pandasql`

### S3 Buckets

* **`nc-crigglestone-ingest-bucket`**: stores ingested CSV files.
* **`nc-crigglestone-processed-bucket`**: stores transformed Parquet files.
* **`nc-crigglestone-lambda-bucket`**: stores update-tracking JSON and exported CSV previews.

### Secrets Manager

* **`Project`**: credentials for the source database.
* **`warehouse-db-credentials`**: credentials for the warehouse database.

### Databases

* **Source Database**: PostgreSQL with operational tables.
* **Warehouse Database**: PostgreSQL schema for dimensional and fact tables.

---

## Lambda Functions

### 1. Ingest Lambda

**Purpose:** Extracts updated rows from the source PostgreSQL database into CSV files in the ingest bucket.

**Key Features:**

* Defines 11 source tables (address, counterparty, currency, etc.).
* Uses Secrets Manager to retrieve credentials.
* Tracks incremental updates with `update_tracking.json`.
* Stores data in S3 under structured paths:

  ```
  s3://nc-crigglestone-ingest-bucket/{table}/{timestamp}.csv
  ```

**Execution:** Triggered manually or via EventBridge schedule.


---

### 2. Transform Lambda

**Purpose:** Transforms raw CSVs into star schema tables and stores them as Parquet in the processed bucket.

**Key Features:**

* Creates dimension tables (`dim_location`, `dim_counterparty`, `dim_currency`, `dim_design`, `dim_payment_type`, `dim_staff`, `dim_transaction`, `dim_date`).
* Creates fact tables (`fact_payment`, `fact_purchase_order`, `fact_sales_order`).
* Deduplicates records and performs joins between source tables.
* Splits timestamps into `date` and `time` components.

**Execution:** Triggered by S3 event on new CSV ingestion or manually.

---

### 3. Load Lambda

**Purpose:** Loads transformed Parquet data into the warehouse and exports CSV previews to S3.

**Key Features:**

* Retrieves warehouse credentials from Secrets Manager.
* Loads Parquet files into PostgreSQL (`public` schema).
* Logs table previews (first 10 rows) in CloudWatch.
* Exports full tables to:

  ```
  s3://nc-crigglestone-lambda-bucket/extracts/{table}.csv
  ```

**Execution:** Triggered by S3 events on processed bucket or manually.


---

## Setup Instructions

1. **Create S3 Buckets**

   * `nc-crigglestone-ingest-bucket`
   * `nc-crigglestone-processed-bucket`
   * `nc-crigglestone-lambda-bucket`

2. **Configure Secrets Manager**

   * `Project` for source DB.
   * `warehouse-db-credentials` for warehouse DB.

3. **Set Up Databases**

   * Source PostgreSQL with operational tables.
   * Warehouse PostgreSQL with dimension/fact schema.

4. **Deploy Lambda Functions**

   * Package code + dependencies.
   * Deploy via AWS CLI/console/CI-CD.
   * Configure triggers:

     * Ingest: scheduled (EventBridge).
     * Transform: S3 event on ingest bucket.
     * Load: S3 event on processed bucket.

5. **Test the Pipeline**

   * Trigger Ingest → verify CSV in ingest bucket.
   * Trigger Transform → verify Parquet in processed bucket.
   * Trigger Load → verify warehouse tables and CSV exports.

---

## Usage

* **Running:** Start with Ingest Lambda; others trigger automatically if events are configured.
* **Monitoring:** Use CloudWatch Logs for execution details and errors.
* **Verification:** Check exported CSV previews in the extracts folder.

---

## Notes

* **Schema Consistency:** Source DB must match expected table list.
* **Performance:** Use batching or Glue for very large datasets.
* **Security:** Restrict IAM permissions to least-privilege.
* **Error Handling:** Implement retries and failover where possible.

---
=======

**Purpose:** Extracts updated rows from the source PostgreSQL database into CSV files in the ingest bucket.

**Key Features:**

* Defines 11 source tables (address, counterparty, currency, etc.).
* Uses Secrets Manager to retrieve credentials.
* Tracks incremental updates with `update_tracking.json`.
* Stores data in S3 under structured paths:

  ```
  s3://nc-crigglestone-ingest-bucket/{table}/{timestamp}.csv
  ```

**Execution:** Triggered manually or via EventBridge schedule.


---

### 2. Transform Lambda

**Purpose:** Transforms raw CSVs into star schema tables and stores them as Parquet in the processed bucket.

**Key Features:**

* Creates dimension tables (`dim_location`, `dim_counterparty`, `dim_currency`, `dim_design`, `dim_payment_type`, `dim_staff`, `dim_transaction`, `dim_date`).
* Creates fact tables (`fact_payment`, `fact_purchase_order`, `fact_sales_order`).
* Deduplicates records and performs joins between source tables.
* Splits timestamps into `date` and `time` components.

**Execution:** Triggered by S3 event on new CSV ingestion or manually.

---

### 3. Load Lambda

**Purpose:** Loads transformed Parquet data into the warehouse and exports CSV previews to S3.

**Key Features:**

* Retrieves warehouse credentials from Secrets Manager.
* Loads Parquet files into PostgreSQL (`public` schema).
* Logs table previews (first 10 rows) in CloudWatch.
* Exports full tables to:

  ```
  s3://nc-crigglestone-lambda-bucket/extracts/{table}.csv
  ```

**Execution:** Triggered by S3 events on processed bucket or manually.


---

## Setup Instructions

1. **Create S3 Buckets**

   * `nc-crigglestone-ingest-bucket`
   * `nc-crigglestone-processed-bucket`
   * `nc-crigglestone-lambda-bucket`

2. **Configure Secrets Manager**

   * `Project` for source DB.
   * `warehouse-db-credentials` for warehouse DB.

3. **Set Up Databases**

   * Source PostgreSQL with operational tables.
   * Warehouse PostgreSQL with dimension/fact schema.

4. **Deploy Lambda Functions**

   * Package code + dependencies.
   * Deploy via AWS CLI/console/CI-CD.
   * Configure triggers:

     * Ingest: scheduled (EventBridge).
     * Transform: S3 event on ingest bucket.
     * Load: S3 event on processed bucket.

5. **Test the Pipeline**

   * Trigger Ingest → verify CSV in ingest bucket.
   * Trigger Transform → verify Parquet in processed bucket.
   * Trigger Load → verify warehouse tables and CSV exports.

---

## Usage

* **Running:** Start with Ingest Lambda; others trigger automatically if events are configured.
* **Monitoring:** Use CloudWatch Logs for execution details and errors.
* **Verification:** Check exported CSV previews in the extracts folder.

---

## Notes

* **Schema Consistency:** Source DB must match expected table list.
* **Performance:** Use batching or Glue for very large datasets.
* **Security:** Restrict IAM permissions to least-privilege.
* **Error Handling:** Implement retries and failover where possible.

---
