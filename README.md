# BlueMar Data Pipeline

## Overview
This repository contains the end-to-end data pipeline for BlueMar, built using modern data engineering tools including Apache Airflow, dbt, Snowflake, Soda.io, and Streamlit. The pipeline automates the ingestion, transformation, validation, and visualization of business-critical data.

The pipeline architecture includes the following stages:
- **Data Ingestion:** Data is extracted from Google Sheets using Airbyte and stored in AWS S3.
- **Preprocessing:** A Lambda function preprocesses the raw data.
- **Data Warehouse:** Data is loaded into Snowflake for storage and transformation.
- **Data Transformation:** dbt processes raw data into staging and modeled layers.
- **Data Validation:** Soda.io checks ensure data quality across pipeline stages.
- **Data Visualization:** A Streamlit dashboard visualizes key metrics and refreshes automatically.
- 
Features
--------

### 1\. **Data Ingestion**

-   **Tool:** Airbyte
-   **Source:** Google Sheets
-   **Destination:** AWS S3
-   **DAG Task:** Automatically triggers Airbyte to move data from Google Sheets to the `bluemar-raw-data` S3 bucket.

### 2\. **Preprocessing**

-   **Tool:** AWS Lambda
-   **Purpose:** Cleans and normalizes raw data.
-   **DAG Task:** Invokes the Lambda function to process raw data.

### 3\. **Data Warehouse**

-   **Tool:** Snowflake
-   **Stages:**
    -   **Raw Layer:** Stores raw ingested data.
    -   **Staging Layer:** Processes initial transformations.
    -   **Modeled Layer:** Delivers final structured data.
-   **DAG Task:** Executes Snowflake `COPY INTO` commands to load data from S3 into the raw layer.

### 4\. **Data Transformation**

-   **Tool:** dbt (Data Build Tool)
-   **Layers:**
    -   **Raw Models:** Reflect raw data directly from Snowflake.
    -   **Staging Models:** Perform cleaning and normalization.
    -   **Modeled Models:** Create final datasets for business use.
-   **DAG Tasks:** Execute `dbt run` commands for each transformation layer.

### 5\. **Data Validation**

-   **Tool:** Soda.io
-   **Purpose:** Validates data integrity and ensures compliance with quality checks.
-   **Configuration:** Uses YAML files defining validation rules.
-   **DAG Task:** Executes Soda CLI checks using an external Python environment.

### 6\. **Data Visualization**

-   **Tool:** Streamlit
-   **Purpose:** Provides a dynamic dashboard for data insights.
-   **DAG Task:** Refreshes the Streamlit dashboard using a REST API.

* * * * *

Installation and Setup
----------------------

### Prerequisites

1.  **Docker and Docker Compose**
    -   Install Docker: Docker Installation Guide
    -   Install Docker Compose: Compose Installation Guide
2.  **Astronomer CLI** (for running Airflow locally)
    -   Install: Astronomer CLI Guide

### Local Setup

1.  Clone the repository:

    `git clone <repository_url>
    cd <repository_directory>`

2.  Install dependencies:

    `pip install -r requirements.txt`

3.  Start the Airflow environment:

    `astro dev start`

4.  Access the Airflow UI:

    -   Navigate to: `http://localhost:8080`
    -   Default credentials: `admin` / `admin`

* * * * *

Airflow DAGs
------------

### `bluemar_pipeline_dag`

This DAG orchestrates the entire pipeline with the following tasks:

1.  **Airbyte Trigger:** Starts the data ingestion from Google Sheets to S3.
2.  **Lambda Preprocessing:** Invokes the AWS Lambda function for data cleaning.
3.  **Snowflake Load:** Loads raw data into Snowflake's raw layer.
4.  **dbt Transformations:** Executes transformations for raw, staging, and modeled layers.
5.  **Soda Check:** Validates data using Soda checks.
6.  **Streamlit Refresh:** Triggers a refresh for the Streamlit dashboard.

* * * * *

Contributing
------------

1.  Fork the repository and create your feature branch:

    `git checkout -b feature/your-feature`

2.  Commit your changes:

    `git commit -m "Add your message here"`

3.  Push to the branch:

    `git push origin feature/your-feature`

4.  Open a pull request.


License
-------

This project is licensed under the MIT License. See the `LICENSE` file for details
