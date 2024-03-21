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



