-- Review unstructured JSON data and diagram a new structured relational data model

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
CREATE OR REPLACE STAGE Products.fetch.receipts_stage
    URL = 's3://habeebanalytics/receipts.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.brands_stage
    URL = 's3://habeebanalytics/brands.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.users_stage
    URL = 's3://habeebanalytics/users.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

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

select count(*) from products.fetch.receipts;
select *
from Products.fetch.brands;

select *
from Products.fetch.users;

--Add the columns/fields in the rewardsReceiptItemList field to the receipt table
-- One by one alter the table and add the column into the rewardsReceiptItemList field to the receipt table

CREATE OR REPLACE TABLE receipt_items AS
SELECT
    r._id AS receipt_id,
    item.value:barcode::VARCHAR AS barcode,
    item.value:description::VARCHAR AS description,
    item.value:finalPrice::FLOAT AS finalPrice,
    item.value:itemPrice::FLOAT AS itemPrice,
    item.value:needsFetchReview::BOOLEAN AS needsFetchReview,
    item.value:partnerItemId::VARCHAR AS partnerItemId,
    item.value:preventTargetGapPoints::BOOLEAN AS preventTargetGapPoints,
    item.value:quantityPurchased::INTEGER AS quantityPurchased,
    item.value:userFlaggedBarcode::VARCHAR AS userFlaggedBarcode,
    item.value:userFlaggedNewItem::BOOLEAN AS userFlaggedNewItem,
    item.value:userFlaggedPrice::FLOAT AS userFlaggedPrice,
    item.value:userFlaggedQuantity::INTEGER AS userFlaggedQuantity
FROM
    receipts r,
    LATERAL FLATTEN(input => r.rewardsReceiptItemList) item;



-- Populate it with the data from the rewardlist items
-- Assuming `receipts` table schema can't directly accommodate multiple item entries and focusing on the first item



-- Creating tables out of the tables, especially receipts and brands that have json like data
-- Receipt Items Table



--Brands Items Table






--Generate a query that answers a predetermined business question
-- 1. What are the top 5 brands by receipts scanned for most recent month?
-- Top 5 Brands by Receipts Scanned for Most Recent Month
SELECT b.NAME, COUNT(*) AS ReceiptsScanned
FROM receipts r
JOIN brands b ON r.brand_id = b._ID  -- Assuming there's a way to associate receipts to brands
WHERE DATE_TRUNC('month', r.DATESCANNED) = DATE_TRUNC('month', CURRENT_DATE() - INTERVAL '1 month')
GROUP BY b.NAME
ORDER BY ReceiptsScanned DESC
LIMIT 5;


-- 2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
-- Ranking of the Top 5 Brands by Receipts Scanned for the Recent Month Compared to the Previous Month
-- Adjust the interval as needed to target the specific "recent" month
SELECT b.NAME, COUNT(*) AS ReceiptsScanned
FROM receipts r
JOIN brands b ON r.brand_id = b._ID
WHERE DATE_TRUNC('month', r.DATESCANNED) = DATE_TRUNC('month', CURRENT_DATE())
GROUP BY b.NAME
ORDER BY ReceiptsScanned DESC
LIMIT 5;


-- 3. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
-- Average Spend from Receipts with 'Accepted' or 'Rejected' Status

SELECT REWARDSRECEIPTSTATUS, AVG(TOTALSPENT) AS AverageSpend
FROM receipts
WHERE REWARDSRECEIPTSTATUS IN ('Accepted', 'Rejected')
GROUP BY REWARDSRECEIPTSTATUS;

-- 4. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
-- Total Number of Items Purchased from Receipts with 'Accepted' or 'Rejected' Status
SELECT REWARDSRECEIPTSTATUS, SUM(PURCHASEDITEMCOUNT) AS TotalItemsPurchased
FROM receipts
WHERE REWARDSRECEIPTSTATUS IN ('Accepted', 'Rejected')
GROUP BY REWARDSRECEIPTSTATUS;


-- 5. Which brand has the most spend among users who were created within the past 6 months?
-- Brand with Most Spend Among Users Created Within the Past 6 Months
SELECT b.NAME, SUM(r.TOTALSPENT) AS TotalSpend
FROM receipts r
JOIN users u ON r.USERID = u._ID
JOIN brands b ON r.brand_id = b._ID  -- Assuming a way to associate receipts to brands
WHERE u.CREATEDDATE > CURRENT_DATE() - INTERVAL '6 months'
GROUP BY b.NAME
ORDER BY TotalSpend DESC
LIMIT 1;

-- 6. Which brand has the most transactions among users who were created within the past 6 months?
-- Brand with Most Transactions Among Users Created Within the Past 6 Months
SELECT b.NAME, COUNT(r._ID) AS Transactions
FROM receipts r
JOIN users u ON r.USERID = u._ID
JOIN brands b ON r.brand_id = b._ID  -- Assuming a way to associate receipts to brands
WHERE u.CREATEDDATE > CURRENT_DATE() - INTERVAL '6 months'
GROUP BY b.NAME
ORDER BY Transactions DESC
LIMIT 1;
