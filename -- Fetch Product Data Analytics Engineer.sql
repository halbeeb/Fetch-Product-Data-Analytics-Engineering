-- Creating warehouse, database, and schema
CREATE WAREHOUSE IF NOT EXISTS Fetch_Rewards
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS Products;

CREATE SCHEMA IF NOT EXISTS Products.fetch;

-- Create a single file format for JSON files, assuming all files are GZIP compressed and in JSON format
CREATE OR REPLACE FILE FORMAT Products.fetch.json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

-- Create stages for uploaded files
CREATE OR REPLACE STAGE Products.fetch.brands_stage
    URL = 's3://habeebanalytics/brands.json'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.users_stage
    URL = 's3://habeebanalytics/users.json'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.receipts_stage
    URL = 's3://habeebanalytics/receipts.json'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

Select * from @products.fetch.receipts_stage (file_format=> 'Products.fetch.json_format');

-- Create tables for our data: Receipts, Users, Brands
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

CREATE TABLE IF NOT EXISTS Products.fetch.users (
    _id VARCHAR,
    state VARCHAR,
    createdDate TIMESTAMP_LTZ,
    lastLogin TIMESTAMP_LTZ,
    role VARCHAR,
    active BOOLEAN
);

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

-- Copy data into tables with match by column name case sensitive, error handling as required
COPY INTO Products.fetch.brands
FROM @Products.fetch.brands_stage
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format)
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
ON_ERROR = 'CONTINUE';

COPY INTO Products.fetch.users
FROM @Products.fetch.users_stage
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format)
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
ON_ERROR = 'CONTINUE';

COPY INTO Products.fetch.receipts
FROM @Products.fetch.receipts_stage
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format)
MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
ON_ERROR = 'CONTINUE';


--Testing new code
-- Populate the receipts with the data from the stage receipts
COPY INTO Products.fetch.receipts (
    _id,
    bonusPointsEarned,
    bonusPointsEarnedReason,
    createDate,
    dateScanned,
    finishedDate,
    modifyDate,
    pointsAwardedDate,
    pointsEarned,
    purchaseDate,
    purchasedItemCount,
    rewardsReceiptItemList,
    rewardsReceiptStatus,
    totalSpent,
    userId
)
FROM (
    SELECT 
        $1:_id:"$oid"::VARCHAR,
        $1:bonusPointsEarned::FLOAT,
        $1:bonusPointsEarnedReason::VARCHAR,
        TO_TIMESTAMP_NTZ($1:createDate:"$date"::NUMBER / 1000),
        TO_TIMESTAMP_NTZ($1:dateScanned:"$date"::NUMBER / 1000),
        TO_TIMESTAMP_NTZ($1:finishedDate:"$date"::NUMBER / 1000),
        TO_TIMESTAMP_NTZ($1:modifyDate:"$date"::NUMBER / 1000),
        TO_TIMESTAMP_NTZ($1:pointsAwardedDate:"$date"::NUMBER / 1000),
        $1:pointsEarned::FLOAT,
        TO_TIMESTAMP_NTZ($1:purchaseDate:"$date"::NUMBER / 1000),
        $1:purchasedItemCount::INT,
        $1:rewardsReceiptItemList::VARIANT,
        $1:rewardsReceiptStatus::VARCHAR,
        $1:totalSpent::FLOAT,
        $1:userId::VARCHAR
    FROM @Products.fetch.receipts_stage
) ON_ERROR = 'CONTINUE'
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);


select *
from Products.fetch.receipts;


select *
from Products.fetch.receipts;

select *
from Products.fetch.users;


Select * from @products.fetch.receipts_stage (file_format=> 'Products.fetch.json_format');

