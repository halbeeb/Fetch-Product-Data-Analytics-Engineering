-- Creating warehouse, database, and schema
CREATE WAREHOUSE IF NOT EXISTS Fetch_Rewards
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

--Create the database name Products
CREATE DATABASE IF NOT EXISTS Products;

--Create the Schema name Fetch
CREATE SCHEMA IF NOT EXISTS Products.fetch;

-- Create a single file format for JSON files, assuming all files are GZIP compressed and in JSON format
CREATE OR REPLACE FILE FORMAT Products.fetch.json_format
    TYPE = 'JSON'
    COMPRESSION = 'GZIP'
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

-- Create stages for uploaded files
CREATE OR REPLACE STAGE Products.fetch.brands_stage
    URL = 's3://habeebanalytics/brands.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.users_stage
    URL = 's3://habeebanalytics/users.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.receipts_stage
    URL = 's3://habeebanalytics/receipts.json.gz'
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


--Copy into Users table from the user stage
COPY INTO Products.fetch.users (
    _id,
    active,
    createdDate,
    lastLogin,
    role,
    state
)
FROM (
    SELECT 
        $1:_id:"$oid"::VARCHAR AS _id,
        $1:active::BOOLEAN AS active,
        TO_TIMESTAMP_NTZ($1:createdDate:"$date"::NUMBER / 1000) AS createdDate,
        TO_TIMESTAMP_NTZ($1:lastLogin:"$date"::NUMBER / 1000) AS lastLogin,
        $1:role::VARCHAR AS role,
        $1:state::VARCHAR AS state
    FROM @Products.fetch.users_stage
)
ON_ERROR = 'CONTINUE'
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);


-- Copy into brands tables from brands stage
COPY INTO Products.fetch.brands (
    _id,
    barcode,
    brandCode,
    category,
    categoryCode,
    cpg,
    topBrand,
    name
)
FROM (
    SELECT 
        $1:_id:"$oid"::VARCHAR AS _id,
        $1:barcode::VARCHAR AS barcode,
        $1:brandCode::VARCHAR AS brandCode,
        $1:category::VARCHAR AS category,
        $1:categoryCode::VARCHAR AS categoryCode,
        OBJECT_CONSTRUCT('id', $1:cpg:"$id":"$oid"::STRING, 'ref', $1:cpg:"$ref"::STRING) AS cpg,
        $1:topBrand::BOOLEAN AS topBrand,
        $1:name::VARCHAR AS name
    FROM @Products.fetch.brands_stage
)
ON_ERROR = 'CONTINUE'
FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

-- Testing the receipts, brands, and users table
select *
from Products.fetch.receipts;

select *
from Products.fetch.brands;

select *
from Products.fetch.users;


Select * from @products.fetch.receipts_stage (file_format=> 'Products.fetch.json_format');

