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
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

CREATE OR REPLACE STAGE Products.fetch.receipts_stage
    URL = 's3://habeebanalytics/receipts.json'
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
    URL = 's3://habeebanalytics/receipts.json'
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



CREATE OR REPLACE STAGE my_json_stage 
    URL = 's3://habeebanalytics/receipts.json'; 




--Testing the s3 access

create or replace stage my_s3_stage
  url='s3://habeebanalytics/brands.json'
  FILE_FORMAT = (TYPE = JSON);

desc stage my_s3_stage;

list @my_s3_stage;

CREATE or replace FILE FORMAT my_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

CREATE or replace TABLE brand_data (
  _id STRING,
  barcode STRING,
  brandCode STRING,
  category STRING,
  categoryCode STRING,
  cpg VARIANT, -- Assuming this could be a complex structure
  topBrand BOOLEAN,
  name STRING
);

CREATE or replace STAGE my_stage
  URL = 's3://habeebanalytics'
  FILE_FORMAT = (FORMAT_NAME = my_json_format);


COPY INTO brand_data
FROM @my_stage/brands.json
FILE_FORMAT = (FORMAT_NAME = my_json_format)
MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
ON_ERROR = 'CONTINUE';


-- Create a staging table
CREATE OR REPLACE TABLE brand_data_staging (raw VARIANT);

-- Copy data into the staging table
COPY INTO brand_data_staging
FROM @my_stage/brands.json
FILE_FORMAT = (FORMAT_NAME = my_json_format)
ON_ERROR = 'CONTINUE';

-- Insert data from the staging table into the structured table
INSERT INTO brand_data
SELECT 
  raw:_id::STRING,
  raw:barcode::STRING,
  raw:brandCode::STRING,
  raw:category::STRING,
  raw:categoryCode::STRING,
  raw:cpg,
  raw:topBrand::BOOLEAN,
  raw:name::STRING
FROM brand_data_staging;


CREATE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

CREATE TABLE med_insured (
  age INT,
  sex VARCHAR,
  bmi FLOAT,
  children INT,
  smoker VARCHAR,
  region VARCHAR,
  charges FLOAT
);



CREATE or replace stage my_s3_stage
  URL='s3://habeebanalytics'
  FILE_FORMAT = my_csv_format;


copy into med_insured (age, sex, bmi, children, smoker, region, charges)
from 's3://habeebanalytics/medical_insurance.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );


-- Receipts table
CREATE or replace TABLE receipts (
  _id VARCHAR,
  bonusPointsEarned FLOAT,
  bonusPointsEarnedReason VARCHAR,
  createDate TIMESTAMP_NTZ,
  dateScanned TIMESTAMP_NTZ,
  finishedDate TIMESTAMP_NTZ,
  modifyDate TIMESTAMP_NTZ,
  pointsAwardedDate TIMESTAMP_NTZ,
  pointsEarned FLOAT,
  purchaseDate TIMESTAMP_NTZ,
  purchasedItemCount INT,
  rewardsReceiptItemList VARIANT,
  rewardsReceiptStatus VARCHAR,
  totalSpent FLOAT,
  userId VARCHAR
);

-- Users table
CREATE or replace TABLE users (
  _id VARCHAR,
  state VARCHAR,
  createdDate TIMESTAMP_NTZ,
  lastLogin TIMESTAMP_NTZ,
  role VARCHAR,
  active BOOLEAN
);

-- Brands table
CREATE or replace TABLE brands (
  _id VARCHAR,
  barcode VARCHAR,
  brandCode VARCHAR,
  category VARCHAR,
  categoryCode VARCHAR,
  cpg VARIANT,
  topBrand BOOLEAN,
  name VARCHAR
);


-- Stage for users.json
CREATE or replace STAGE users_stage
  URL='s3://habeebanalytics/users.json'
  FILE_FORMAT = (FORMAT_NAME = my_json_format);

-- Stage for brands.json
CREATE or replace STAGE brands_stage
  URL='s3://habeebanalytics/brands.json'
  FILE_FORMAT = (FORMAT_NAME = my_json_format);

-- Stage for receipts.json
CREATE or replace STAGE receipts_stage
  URL='s3://habeebanalytics/receipts.json'
  FILE_FORMAT = (FORMAT_NAME = my_json_format);


-- Load users data
COPY INTO users
FROM @users_stage
FILE_FORMAT = (FORMAT_NAME = my_json_format)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;

-- Load brands data
COPY INTO brands
FROM @brands_stage
FILE_FORMAT = (FORMAT_NAME = my_json_format)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;

-- Load receipts data
COPY INTO receipts
FROM @receipts_stage
FILE_FORMAT = (FORMAT_NAME = my_json_format)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;

Create or replace stage my_s3_stage
    url ='s3://habeebanalytics';

desc stage my_s3_stage;

list @my_s3_stage;

select * 
from @my_s3_stage/brands.json (file_format=>my_json_format);

medical_insurance.csv
shows file format in my_s3_stage;
