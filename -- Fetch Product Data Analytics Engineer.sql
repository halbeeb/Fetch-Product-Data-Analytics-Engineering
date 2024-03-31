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

select *
from Products.fetch.brands;

select *
from Products.fetch.users;

-- Creating tables out of the tables, especially receipts and brands that have json like data
-- Receipt Items Table
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

--View the receipt items table just create
select * from receipt_items;

--Brands Items Table
CREATE OR REPLACE TABLE brand_cpg_details AS
SELECT
    b._id AS brand_id,
    b.name AS brand_name,
    b.CPG:id::STRING AS cpg_id,
    b.CPG:ref::STRING AS cpg_ref
FROM
    brands b;

-- View the brand_cpg_details just created and examine it
Select * from brand_cpg_details;
select * from brands;
select * from receipts;
Select * from users;
select * from receipt_items;


/* --Generate a query that answers a predetermined business question
-- 1. What are the top 5 brands by receipts scanned for most recent month?
-- Top 5 Brands by Receipts Scanned for Most Recent Month
SELECT ri.barcode, COUNT(*) AS ReceiptsScanned
FROM receipts r
JOIN Receipt_items ri on ri.receipt_id=r._id
WHERE DATE_TRUNC('month', r.DATESCANNED) = DATE_TRUNC('month', CURRENT_DATE() - INTERVAL '1 month')
GROUP BY ri.barcode
ORDER BY ReceiptsScanned DESC
LIMIT 5;

select b.name AS BrandName, count(datescanned) AS ReceiptsScanned
from receipts r
JOIN receipt_items ri ON ri.receipt_id = r._id
JOIN brands b ON ri.barcode = b.barcode
WHERE DATE_TRUNC('month', r.DATESCANNED) = (
    SELECT DATE_TRUNC('month', MAX(r2.DATESCANNED))
    FROM receipts r2
)
Group by b.name
ORDER BY ReceiptsScanned DESC
LIMIT 5;


-- 2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
-- Ranking of the Top 5 Brands by Receipts Scanned for the Recent Month Compared to the Previous Month
-- Adjust the interval as needed to target the specific "recent" month

WITH LatestDate AS (
    SELECT DATE_TRUNC('month', MAX(DATESCANNED)) AS LatestMonth
    FROM receipts
),
RecentMonth AS (
    SELECT 
        b.name AS BrandName,
        COUNT(*) AS ReceiptsScanned,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS RankRecent
    FROM receipts r
    JOIN receipt_items ri ON ri.receipt_id = r._id
    JOIN brands b ON ri.barcode = b.barcode,
    LatestDate ld
    WHERE DATE_TRUNC('month', r.DATESCANNED) = ld.LatestMonth
    GROUP BY b.name
    LIMIT 5
),
PreviousMonth AS (
    SELECT 
        b.name AS BrandName,
        COUNT(*) AS ReceiptsScanned,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS RankPrevious
    FROM receipts r
    JOIN receipt_items ri ON ri.receipt_id = r._id
    JOIN brands b ON ri.barcode = b.barcode,
    LatestDate ld
    WHERE DATE_TRUNC('month', r.DATESCANNED) = DATEADD(month, -1, ld.LatestMonth)
    GROUP BY b.name
    LIMIT 5
)
SELECT 
    rm.BrandName,
    rm.RankRecent,
    pm.RankPrevious
FROM RecentMonth rm
LEFT JOIN PreviousMonth pm ON rm.BrandName = pm.BrandName
ORDER BY rm.RankRecent;

*/ 

-- Query 1
-- When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
-- Average Spend from Receipts with 'Accepted' or 'Rejected' Status

-- Assumed that FINISHED and SUBMITED are ACCEPTED while FLAGGED and REJECTED are REJECTED while other stay as they are:
SELECT 
    CASE
        WHEN REWARDSRECEIPTSTATUS IN ('FINISHED', 'SUBMITTED') THEN 'ACCEPTED'
        WHEN REWARDSRECEIPTSTATUS IN ('REJECTED', 'FLAGGED') THEN 'REJECTED'
        ELSE REWARDSRECEIPTSTATUS
    END AS "Reward Receipt Status", 
    ROUND(AVG(TOTALSPENT), 2) AS "Average Spend"
FROM 
    receipts
WHERE 
    REWARDSRECEIPTSTATUS IN ('FINISHED', 'SUBMITTED', 'REJECTED', 'FLAGGED')
    AND TOTALSPENT IS NOT NULL
GROUP BY 
    "Reward Receipt Status"
ORDER BY 
    "Average Spend" DESC;



-- Query 2
-- When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
-- Total Number of Items Purchased from Receipts with 'Accepted' or 'Rejected' Status

-- Assumed that FINISHED and SUBMITED are ACCEPTED while FLAGGED and REJECTED are REJECTED while other stay as they are
SELECT 
    CASE
        WHEN REWARDSRECEIPTSTATUS IN ('FINISHED', 'SUBMITTED') THEN 'ACCEPTED'
        WHEN REWARDSRECEIPTSTATUS IN ('REJECTED', 'FLAGGED') THEN 'REJECTED'
        ELSE REWARDSRECEIPTSTATUS
    END AS "Rewards Receipt Status", 
    SUM(purchasedItemCount) AS "Total Items Purchased"
FROM 
    receipts
WHERE 
    REWARDSRECEIPTSTATUS IN ('FINISHED', 'SUBMITTED', 'REJECTED', 'FLAGGED')
    AND purchasedItemCount IS NOT NULL
GROUP BY 
    "Rewards Receipt Status"
ORDER BY 
    "Total Items Purchased" DESC;


-- Query 3
-- Which brand has the most spend among users who were created within the past 6 months?
-- Brand with Most Spend Among Users Created Within the Past 6 Months
WITH LatestUserDate AS (
    SELECT MAX(CAST(createdDate AS DATE)) AS MaxCreateDate
    FROM Users
),
RecentUsers AS (
    SELECT _id
    FROM Users
    WHERE createdDate >= DATEADD(month, -6, (SELECT MaxCreateDate FROM LatestUserDate))
),
BrandSpend AS (
    SELECT 
        b.name AS BrandName,
        SUM(ri.finalPrice) AS TotalBrandSpend
    FROM Receipts r
    JOIN RecentUsers ru ON ru._id = r.userId
    JOIN Receipt_Items ri ON r._id = ri.receipt_Id
    JOIN Brands b ON ri.barcode = b.barcode
    GROUP BY b.name
)
SELECT BrandName, TotalBrandSpend
FROM BrandSpend
ORDER BY TotalBrandSpend DESC
LIMIT 1;

-- Query 4
-- Which brand has the most transactions among users who were created within the past 6 months?
-- Brand with Most Transactions Among Users Created Within the Past 6 Months

WITH LatestUserDate AS (
    SELECT MAX(CAST(createdDate AS DATE)) AS MaxCreateDate
    FROM Users
),
RecentUsers AS (
    SELECT _id
    FROM Users
    WHERE CAST(createdDate AS DATE) >= DATEADD(month, -6, (SELECT MaxCreateDate FROM LatestUserDate))
),
UserTransactions AS (
    SELECT 
        r.userId, 
        ri.barcode,
        COUNT(r._id) AS Transactions
    FROM Receipts r
    JOIN Users ru ON r.userId = ru._id
    JOIN Receipt_Items ri ON r._id = ri.receipt_Id
    GROUP BY r.userId, ri.barcode
),
BrandTransactions AS (
    SELECT 
        b.name AS BrandName,
        SUM(ut.Transactions) AS TotalTransactions
    FROM UserTransactions ut
    JOIN Brands b ON ut.barcode = b.barcode
    GROUP BY b.name
)
SELECT BrandName as"Brand Name", TotalTransactions as "Total Transactions"
FROM BrandTransactions
ORDER BY TotalTransactions DESC
LIMIT 1;


--Further examination of the source file for data quality issues
-- For users
SELECT
    COUNT(*) AS total_rows,
    COUNT(STATE) AS non_missing_state,
    COUNT(LASTLOGIN) AS non_missing_lastlogin
FROM "USERS";

-- For brands
SELECT
    COUNT(*) AS total_rows,
    COUNT(BRANDCODE) AS non_missing_brandcode,
    COUNT(CATEGORY) AS non_missing_category,
    COUNT(CATEGORYCODE) AS non_missing_categorycode,
    COUNT(TOPBRAND) AS non_missing_topbrand
FROM "BRANDS";

-- For receipts
SELECT
    COUNT(*) AS total_rows,
    COUNT(BONUSPOINTSEARNED) AS non_missing_bonuspointsearned,
    COUNT(BONUSPOINTSEARNEDREASON) AS non_missing_bonuspointsearnedreason,
    COUNT(CREATEDATE) AS non_missing_createdate,
    COUNT(DATESCANNED) AS non_missing_datescanned,
    COUNT(FINISHEDDATE) AS non_missing_finisheddate,
    COUNT(MODIFYDATE) AS non_missing_modifydate,
    COUNT(POINTSAWARDEDDATE) AS non_missing_pointsawardeddate,
    COUNT(POINTSEARNED) AS non_missing_pointsearned,
    COUNT(PURCHASEDATE) AS non_missing_purchasedate,
    COUNT(PURCHASEDITEMCOUNT) AS non_missing_purchaseditemcount,
    COUNT(REWARDSRECEIPTITEMLIST) AS non_missing_rewardsreceiptitemlist,
    COUNT(REWARDSRECEIPTSTATUS) AS non_missing_rewardsreceiptstatus,
    COUNT(TOTALSPENT) AS non_missing_totalspent,
    COUNT(USERID) AS non_missing_userid
FROM "RECEIPTS";

-- For receipt_items
SELECT
    COUNT(*) AS total_rows,
    COUNT(RECEIPT_ID) AS non_missing_receipt_id,
    COUNT(BARCODE) AS non_missing_barcode,
    COUNT(DESCRIPTION) AS non_missing_description,
    COUNT(FINALPRICE) AS non_missing_finalprice,
    COUNT(ITEMPRICE) AS non_missing_itemprice,
    COUNT(NEEDSFETCHREVIEW) AS non_missing_needsfetchreview,
    COUNT(PARTNERITEMID) AS non_missing_partneritemid,
    COUNT(PREVENTTARGETGAPPOINTS) AS non_missing_preventtargetgapoints,
    COUNT(QUANTITYPURCHASED) AS non_missing_quantitypurchased,
    COUNT(USERFLAGGEDBARCODE) AS non_missing_userflaggedbarcode,
    COUNT(USERFLAGGEDNEWITEM) AS non_missing_userflaggednewitem,
    COUNT(USERFLAGGEDPRICE) AS non_missing_userflaggedprice,
    COUNT(USERFLAGGEDQUANTITY) AS non_missing_userflaggedquantity
FROM "RECEIPT_ITEMS";

-- For brand_cpg_details
SELECT
    COUNT(*) AS total_rows,
    COUNT(BRAND_ID) AS non_missing_brandid,
    COUNT(CPG_ID) AS non_missing_cpgid
FROM "BRAND_CPG_DETAILS";

--End of the queries
