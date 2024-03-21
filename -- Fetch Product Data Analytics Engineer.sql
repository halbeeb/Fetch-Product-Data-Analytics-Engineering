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

-- Create Stages for Uploaded Files
CREATE OR REPLACE STAGE Products.fetch.brands_stage
    URL = 's3://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/brands.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.users_stage
    URL = 's3://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/users.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

CREATE OR REPLACE STAGE Products.fetch.receipts_stage
    URL = 's3://fetch-hiring.s3.amazonaws.com/analytics-engineer/ineeddata-data-modeling/receipts.json.gz'
    FILE_FORMAT = (FORMAT_NAME = Products.fetch.json_format);

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

