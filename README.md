# Fetech-Product-Data-Analytics-Engineering

This project focuses on analyzing and structuring unstructured JSON data provided by Fetch Rewards, a hypothetical company. The main objectives are to design a structured relational data model, write SQL queries to answer specific business questions, identify and address data quality issues, and communicate findings effectively to stakeholders. Basically, there are four main requirements that this projects aims satisfy and they are as follow:

> 1. **Review of Unstructured Data and Relational Data Modeling**
 
 #### Data Sources
 [Receipts](https://habeebanalytics.s3.eu-north-1.amazonaws.com/receipts.json.gz)
  S3 URI s3://habeebanalytics/receipts.json.gz
 [Brands](https://habeebanalytics.s3.eu-north-1.amazonaws.com/brands.json.gz)
  S3 URI: s3://habeebanalytics/brands.json.gz
 [Users](https://habeebanalytics.s3.eu-north-1.amazonaws.com/users.json.gz)
  S3 URI: s3://habeebanalytics/users.json.gz

> 2. **SQL Query That Answers Four Predetermined Business Question**
    The four predetermined business questions are:
    > i. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
    ii. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater? 
    iii. Which brand has the most spend among users who were created within the past 6 months?
    iv. Which brand has the most transactions among users who were created within the past 6 months?
   
> 3. **Data Quality Evaluation**
> 4. **Stakeholder Communication**
   
#### Analytical Tools Used
**Cloud Data Warehouse: Snowflake
Cloud Object Storage: AWS S3 Bucket
Version Control: Git Bash
Repository: Github**


## Review of Unstructured Data and Relational Data Modeling
Given the receipts, brands and users data, and the receipts_items and brand cgp detail from receipts and brands respectively, below show the relational data modelling designed in the Snowflake cloud data warehouse and equally obtainable in other cloud data warehouse:

**Product.Fetch Relational Data Modeling
[ER Diagram]

## SQL Query That Answers Four Predetermined Business Question**
  given the relational above and the assumptions earlier stated, the four predetermined business are thus answered below:

**When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?**



**When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?**



**Which brand has the most spend among users who were created within the past 6 months?**



**Which brand has the most transactions among users who were created within the past 6 months?**



## Data Quality Evaluation

#### Untidiness (Structural Issues)

receipts:
Contains both receipt-level and item-level information in a single table, suggesting a need for normalization.
General:
No other immediate structural issues are apparent without more context on the data relationships. However, the potential overlap between brands and brand_cpg_details could be considered here once the relationship is clearer.

#### Messiness (Content Issues)

users:
Incorrect data types for date columns (CREATEDDATE, LASTLOGIN).
brands:
TOPBRAND column uses object data type possibly due to missing values, which may complicate analysis involving this boolean-like variable.
receipts:
Mixed data types for columns that appear to be boolean (NEEDSFETCHREVIEW, ITEM_USERFLAGGEDNEWITEM), and numeric columns used for identifiers are in float64 due to NaNs, suggesting messy data entry or processing.
receipt_items:
Mixed types for columns that seem to represent boolean values (NEEDSFETCHREVIEW, USERFLAGGEDNEWITEM).
Inconsistent handling of missing data, with numerous missing values in user-flagged columns and BARCODE.

#### Completeness and Integrity
users:
Missing values in STATE and LASTLOGIN.
Duplicate rows identified.
brands:
Significant missing values in BRANDCODE, CATEGORY, CATEGORYCODE, and TOPBRAND.
receipts:
Substantial missing values across various columns, particularly item-related ones.
receipt_items:
Extensive missing data, particularly in BARCODE and user-flagged columns, which may affect completeness and data quality.
brand_cpg_details:
Appears complete with no missing values or duplicate rows, suggesting good integrity for this dataset. However, the relationship with brands needs to be checked for redundancy and consistency.
These categories highlight key areas for data cleaning and preparation. Addressing these issues will be crucial for ensuring the datasets are in a usable state for analysis.

## Stakeholder Communication

