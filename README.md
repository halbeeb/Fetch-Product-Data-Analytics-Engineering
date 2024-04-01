# Fetech-Product-Data-Analytics-Engineering

This project focuses on analyzing and structuring unstructured JSON data provided by Fetch Rewards, a hypothetical company. The main objectives are to design a structured relational data model, write SQL queries to answer specific business questions, identify and address data quality issues, and communicate findings effectively to stakeholders. Basically, there are four main requirements that this projects aims satisfy and they are as follow:

#### 1. Review of Unstructured Data and Relational Data Modeling
 
 #### Data Sources
 
 [Receipts](https://habeebanalytics.s3.eu-north-1.amazonaws.com/receipts.json.gz)
  S3 URI s3://habeebanalytics/receipts.json.gz
 
 [Brands](https://habeebanalytics.s3.eu-north-1.amazonaws.com/brands.json.gz)
  S3 URI: s3://habeebanalytics/brands.json.gz
 
 [Users](https://habeebanalytics.s3.eu-north-1.amazonaws.com/users.json.gz)
  S3 URI: s3://habeebanalytics/users.json.gz

#### 2. SQL Query That Answers Four Predetermined Business Question
    The four predetermined business questions are:
    > i. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
    ii. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater? 
    iii. Which brand has the most spend among users who were created within the past 6 months?
    iv. Which brand has the most transactions among users who were created within the past 6 months?
   
#### 3. Data Quality Evaluation
#### 4. Stakeholder Communication
   
#### Analytical Tools Used

- Cloud Data Warehouse: Snowflake
- Cloud Object Storage: AWS S3 Bucket
- Version Control: Git Bash
- Repository: Github

### Assumptions

1. **Fetch_Rewards Data Warehouse:** Designed for small-scale operations with a product-centric analytics approach, housed in the `fetch` schema within the `Products` database.
2. **Data Ingestion from S3:** Utilizes stages for ingesting semi-structured JSON data from S3 buckets, indicating reliance on cloud storage fo r data collectio. **Data Format Choices:** 
   - JSON file format for semi-structured data likely from NoSQL databases or web applications.
   - GZIP compression for storage and transfer efficiency.
   - Format options like STRIP_OUTER_ARRAY suggest the need for JSON data cleanup during ingestion.
4. **Table Structure and Relationships:** 
   - Core tables: `receipts`, `users`, and `brands`, with relationships mapped in ER diagrams.
   - `receipts` table has a `VARIANT` column (`rewardsReceiptItemList`) for nested JSON, necessitating flattening into `receipt_items`.
   - `brands` table's `cpg` field contains nested JSON (`id` and `ref`), leading to parsing into `brand_cpg_details` for normalized analysis.
   - Link between `receipts` and `users` through `userId` in `receipts`, showing each receipt's user connection.
5. **Data Copy and Transformation:** 
   - Use of `COPY INTO` command for error-resilient data loading.
   - `TO_TIMESTAMP_NTZ` for normalizing JSON dates into a query-friendly timestamp format.
   - Transformation of nested JSON (`cpg`) into structured format within the `brands` table.
6. **Queries for Business Intelligence:** 
   - Focus on consumer behavior, brand performance, and temporal sales trends.
   - Interest in month-over-month data comparison to monitor market dynamics and consumer engagement.
7. **Data Quality and Exploration:** 
   - Some part of the quality data check were performed through observations
8. **Receipt Status Interpretation:** 
   - Receipt statuses `Finished` and `Submitted` are considered `ACCEPTED`.
   - Statuses `Flagged` and `Rejected` are considered `REJECTED`.
9.  The data is presummed not to change at any time.


## Review of Unstructured Data and Relational Data Modeling
Given the receipts, brands and users data, and the receipts_items and brand cgp detail from receipts and brands respectively, below show the relational data modelling designed in the Snowflake cloud data warehouse and equally obtainable in other cloud data warehouse:

#### Product.Fetch Relational Data Modeling/ Entity Relationship Diagram

![ER Diagram.png](https://github.com/halbeeb/Fetch-Product-Data-Analytics-Engineering/blob/main/ER%20Diagram.png)

## SQL Queries for Predetermined Business Question

given the relational above and the assumptions earlier stated, the four predetermined business are thus answered below:

#### 1. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

``` SQL
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

```


REWARD RECEIPT STATUS  | AVERAGE SPEND
-----------------------|-----------------------
REJECTED | 85.10
ACCEPTED | 80.85

> It is evident as seen from the table that the rejected is greater than the Accepted on average.


#### 2. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

```sql
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
```

REWARD RECEIPT STATUS  | TOTAL ITEMS PURCHASED
-----------------------|-----------------------
ACCEPTED | 8184
REJECTED | 1187
              
> From the table above, ACCEPTED is greater when considering the number of items purchased from receipts.

#### 3. Which brand has the most spend among users who were created within the past 6 months?

```sql
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
SELECT BrandName as "Brand Name", TotalBrandSpend as "Total Brand Spend"
FROM BrandSpend
ORDER BY TotalBrandSpend DESC
LIMIT 1;

```


BRAND NAME            | TOTAL BRAND SPEND
----------------------|---------------------
Cracker Barrel Cheese | 253.26

> As seen from above, *Cracker Barrel Cheese* brand has the most spend among the users created within past 6 months.


#### 4. Which brand has the most transactions among users who were created within the past 6 months?
``` SQL
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
```

BRAND NAME            | TOTAL TRANSACTIONS
----------------------|---------------------
Tostitos              | 43

> The brand with most transactions within past 6 months is **Tostitos**


## Data Quality Evaluation

#### Untidiness (Structural Issu

es)

**receipts:**
[x] Contains both receipt-level and item-level information in a single table, suggesting a need for normalization.
General:
[x] No other immediate structural issues are apparent.

#### Messiness (Content Issues)

**users:**
[x] Incorrect data types for date columns (CREATEDDATE, LASTLOGIN).

**brands:**
[x] TOPBRAND column uses object data type possibly due to missing values, which may complicate analysis involving this boolean-like variable.
receipts:
[x] Mixed data types for columns that appear to be boolean (NEEDSFETCHREVIEW, ITEM_USERFLAGGEDNEWITEM), and numeric columns used for identifiers are in decimals due to NaNs, suggesting messy data entry or processing.
receipt_items:
[x] Mixed types for columns that seem to represent boolean values (NEEDSFETCHREVIEW, USERFLAGGEDNEWITEM).
[x] Inconsistent handling of missing data, with numerous missing values in user-flagged columns and BARCODE.

#### Completeness and Integrity

**users:**
[x] Missing values in STATE and LASTLOGIN.
[x] Duplicate rows identified.

**brands:**
[x] Significant missing values in BRANDCODE, CATEGORY, CATEGORYCODE, and TOPBRAND.
**receipts:**
[x] Substantial missing values across various columns, particularly item-related ones.
**receipt_items:**
[x] Extensive missing data, particularly in BARCODE and user-flagged columns.
**brand_cpg_details:**
[x] Appears complete with no missing values or duplicate rows.


## Stakeholder Communication
