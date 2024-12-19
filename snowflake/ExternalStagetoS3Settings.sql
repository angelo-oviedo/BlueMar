USE ROLE BLUEMAR_DEV_ELT_DEVELOPER_ROLE;
USE WAREHOUSE BLUEMAR_DEV_ELT_WH;
USE DATABASE BLUEMAR_DEV_DB;
USE SCHEMA RAW;

USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE bluemar_dev_elt_developer_role;
USE ROLE BLUEMAR_DEV_ELT_DEVELOPER_ROLE;

CREATE STORAGE INTEGRATION bluemar_raw_data_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '${ARN_ROLE}'
STORAGE_ALLOWED_LOCATIONS = ('s3://bluemar-raw-data/');

DESCRIBE INTEGRATION bluemar_raw_data_s3_integration;

CREATE OR REPLACE STAGE bluemar_raw_data_external_stage
URL = 's3://bluemar-raw-data/'
STORAGE_INTEGRATION = bluemar_raw_data_s3_integration;

list @bluemar_raw_data_external_stage;

CREATE OR REPLACE FILE FORMAT bluemar_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('', 'NULL');

-- The data was loaded into the tables using the UI

SELECT * FROM raw_gastos_operativos LIMIT 10;
SELECT * FROM raw_ingresos LIMIT 10;
SELECT * FROM raw_pago_planillas lIMIT 10;
SELECT * FROM RAW_INVENTARIO_DE_ACTIVOS LIMIT 10;

-- Processed Data

USE ROLE bluemar_dev_elt_developer_role;

CREATE OR REPLACE STAGE bluemar_processed_data_external_stage
    URL='S3://bluemar-processed-data/'
    CREDENTIALS=(AWS_KEY_ID='{KEY_ID}' AWS_SECRET_KEY='{SECRET_KEY}');

ls @bluemar_processed_data_external_stage;


-- The data was loaded into the tables using the UI


USE ROLE BLUEMAR_DEV_ELT_DEVELOPER_ROLE;
USE WAREHOUSE BLUEMAR_DEV_ELT_WH;
USE DATABASE BLUEMAR_DEV_DB;
USE SCHEMA RAW;

drop integration bluemar_processed_data_s3_integration;

CREATE STORAGE INTEGRATION bluemar_processed_data_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '{ARN_ROLE}'
STORAGE_ALLOWED_LOCATIONS = ('s3://bluemar-processed-data/');

DESCRIBE INTEGRATION bluemar_processed_data_s3_integration;

drop stage bluemar_processed_data_external_stage;

CREATE OR REPLACE STAGE bluemar_processed_data_external_stage
URL = 's3://bluemar-processed-data/'
STORAGE_INTEGRATION = bluemar_processed_data_s3_integration;

list @bluemar_raw_data_external_stage;







