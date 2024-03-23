-- Fetch Product Data Analytics Engineering
--Creating warehouse, database and schemas
CREATE WAREHOUSE IF NOT EXISTS Fetch_Rewards
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

--Create database name products
CREATE DATABASE IF NOT EXISTS Products;

-- creating schema for the products database
CREATE SCHEMA IF NOT EXISTS Products.fetch;

-- Create File Format for JSON files
CREATE OR REPLACE FILE FORMAT Products.fetch.json_format
    TYPE = 'JSON'
    COMPRESSION = 'GZIP'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/Habeeb'
  STORAGE_ALLOWED_LOCATIONS = ('s3://fetch-hiring/analytics-engineer/ineeddata-data-modeling/');

CREATE OR REPLACE STAGE Products.fetch.receipts_stage
    URL = 's3://fetch-hiring/analytics-engineer/ineeddata-data-modeling/brands.json.gz'
    STORAGE_INTEGRATION = s3_integration
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

LIST @Products.fetch.receipts_stage;




-- Create Stages for Uploaded Files
CREATE OR REPLACE STAGE Products.fetch.brands_stage
    URL = 's3://fetch-hiring/analytics-engineer/ineeddata-data-modeling/brands.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.users_stage
    URL = 's3://fetch-hiring/analytics-engineer/ineeddata-data-modeling/users.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.receipts_stage
    URL = 's3://fetch-hiring/analytics-engineer/ineeddata-data-modeling/receipts.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

describe Products.fetch.brands_stage;

-- Create tables for our data
-- Receipts Table
CREATE TABLE IF NOT EXISTS Products.fetch.receipts (
    _id VARCHAR,
    bonusPointsEarned FLOAT,
    bonusPointsEarnedReason VARCHAR,
    createDate TIMESTAMP_LTZ,
    dateScanned TIMESTAMP_LTZ,
    finishedDate TIMESTAMP_LTZ,
    modifyDate TIMESTAMP_LTZ,
    pointsAwardedDate TIMESTAMP_LTZ,
    pointsEarned FLOAT,
    purchaseDate TIMESTAMP_LTZ,
    purchasedItemCount INT,
    rewardsReceiptItemList VARIANT,
    rewardsReceiptStatus VARCHAR,
    totalSpent FLOAT,
    userId VARCHAR
);

-- Users Table
CREATE TABLE IF NOT EXISTS Products.fetch.users (
    _id VARCHAR,
    state VARCHAR,
    createdDate TIMESTAMP_LTZ,
    lastLogin TIMESTAMP_LTZ,
    role VARCHAR,
    active BOOLEAN
);

-- Brand Table
CREATE TABLE IF NOT EXISTS Products.fetch.brands (
    _id VARCHAR,
    barcode VARCHAR,
    brandCode VARCHAR,
    category VARCHAR,
    categoryCode VARCHAR,
    cpg VARIANT,
    topBrand BOOLEAN,
    name VARCHAR
);

-- Copy Data into Tables
COPY INTO Products.fetch.brands
FROM @Products.fetch.brands_stage
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

COPY INTO Products.fetch.users
FROM @Products.fetch.users_stage
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

COPY INTO Products.fetch.receipts
FROM @Products.fetch.receipts_stage
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);


-- -- Copy Data into Tables
COPY INTO Products.fetch.brands
FROM @Products.fetch.brands_stage
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format)
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE;

COPY INTO Products.fetch.users
FROM @Products.fetch.users_stage
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format)
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE;

COPY INTO Products.fetch.receipts
FROM @Products.fetch.receipts_stage
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format)
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE;


-- Testing the

CREATE OR REPLACE FILE FORMAT Products.fetch.json_format
    TYPE = 'JSON'
    COMPRESSION = 'GZIP'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = TRUE;


CREATE OR REPLACE STAGE Products.fetch.receipts_stage
    URL = 's3://fetch-hiring/analytics-engineer/ineeddata-data-modeling/'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);



Create or replace test_table (JSON Variant);



CREATE OR REPLACE STAGE Products.fetch.receipts_stage
    URL = 's3://fetch-hiring/analytics-engineer/ineeddata-data-modeling/receipts.json.gz'
    FILE_FORMAT = (type=json, compression=GZIP);

Failure using stage area. Cause: [Access Denied (Status Code: 403; Error Code: AccessDenied)]


--Testing communication with IAM AWS instance
SELECT SYSTEM$GET_AWS_S3_PRE_SIGNED_URL('s3://snowflake-docs/warehouse-provisioning.png'); 



CREATE OR REPLACE FILE FORMAT my_json_format
    TYPE = 'JSON'
    COMPRESSION = 'GZIP'
    STRIP_OUTER_ARRAY = FALSE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = TRUE;


CREATE OR REPLACE STAGE my_stage
    URL = 's3://fetch-hiring/analytics-engineer/ineeddata-data-modeling/brands.json.gz'
    FILE_FORMAT = (FORMAT_NAME = my_json_format);

CREATE OR REPLACE TABLE receipts (
    _id VARCHAR,
    bonusPointsEarned NUMBER,
    bonusPointsEarnedReason STRING,
    createDate TIMESTAMP,
    dateScanned TIMESTAMP,
    finishedDate TIMESTAMP,
    modifyDate TIMESTAMP,
    pointsAwardedDate TIMESTAMP,
    pointsEarned NUMBER,
    purchaseDate TIMESTAMP,
    purchasedItemCount NUMBER,
    rewardsReceiptItemList VARIANT,
    rewardsReceiptStatus STRING,
    totalSpent NUMBER,
    userId STRING
);

COPY INTO receipts
FROM @my_stage/receipts.json.gz
FILE_FORMAT = (FORMAT_NAME = my_json_format)
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
ON_ERROR = 'CONTINUE';


