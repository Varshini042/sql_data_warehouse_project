/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-----------------------
--checking for nulls and duplicates

select * from [bronze].[crm_cust_info]

SELECT cst_id,COUNT(*)
FROM [bronze].[crm_cust_info]
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL

--cleaning nulls and duplicates

SELECT * FROM(SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY [cst_create_date] DESC) AS flag_last
from [bronze].[crm_cust_info]
where cst_id IS not NULL
) t
WHERE flag_last = 1   

--checking unwanted spaces

select [cst_firstname]
FROM [bronze].[crm_cust_info]

--select [cst_firstname]
--FROM [bronze].[crm_cust_info]
--where [cst_firstname] != 
--TRIM([cst_firstname])
----LTRIM(RTRIM([cst_firstname]))

SELECT '[' + [cst_firstname] +']'
FROM [bronze].[crm_cust_info]
WHERE [cst_firstname] LIKE ' %' 
   OR [cst_firstname] LIKE '% '


SELECT '[' + [cst_lastname] +']'
FROM [bronze].[crm_cust_info]
WHERE [cst_lastname] LIKE ' %' 
   OR [cst_lastname] LIKE '% '

/*
Trail
SELECT '[' + [cst_lastname] + ']' AS LastNameWithBrackets,
       LEN([cst_lastname]) AS StringLength,
       DATALENGTH([cst_lastname]) AS DataLength
FROM [bronze].[crm_cust_info]

SELECT [cst_lastname]
FROM [bronze].[crm_cust_info]
WHERE DATALENGTH([cst_lastname]) > LEN([cst_lastname]) * 2*/ 

SELECT [cst_gndr]
FROM [bronze].[crm_cust_info]
WHERE [cst_gndr] LIKE ' %' 
   OR [cst_gndr] LIKE '% '

SELECT [cst_marital_status]
FROM [bronze].[crm_cust_info]
WHERE [cst_marital_status] LIKE ' %' 
   OR [cst_marital_status] LIKE '% '

--cleaning  unwanted spaces

SELECT [cst_id],
[cst_key],
LTRIM(RTRIM([cst_firstname])) as cst_firstname,
LTRIM(RTRIM([cst_lastname])) as cst_lastname,
CASE WHEN UPPER(LTRIM(RTRIM([cst_marital_status]))) = 'S' THEN 'Single'
WHEN UPPER(LTRIM(RTRIM([cst_marital_status]))) = 'M' THEN 'Married'
ELSE 'n/a'  END AS cst_marital_status,
CASE WHEN UPPER(LTRIM(RTRIM([cst_gndr]))) = 'F' THEN 'Female'
WHEN UPPER(LTRIM(RTRIM([cst_gndr]))) = 'M' THEN 'Male'
ELSE 'n/a'  END AS cst_gndr,
cast ([cst_create_date] as DATE) AS cst_create_date
FROM [bronze].[crm_cust_info]


-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results


-- CLEANING & LOADING [crm_prd_info] BRONZE TO SILVER

SELECT [prd_id],
[prd_key],
[prd_nm],
[prd_cost],
[prd_line],
[prd_start_dt],
[prd_end_dt]
FROM [bronze].[crm_prd_info]

select * from [bronze].[erp_px_cat_g1v2]

select * from [bronze].[crm_sales_details]

-- checking nulls or duplicates in primary key i.e prd_id

SELECT [prd_id],COUNT(*)
FROM [bronze].[crm_prd_info]
GROUP BY [prd_id]
HAVING COUNT(*)>1 OR [prd_id] IS NULL

--checking for unwanted spaces
  
SELECT '[' + [prd_nm] +']'
FROM [bronze].[crm_prd_info]
WHERE [prd_nm] LIKE ' %' 
   OR [prd_nm] LIKE '% '

--checking for nulls or negavtive
  
select [prd_cost] FROM [bronze].[crm_prd_info]
where [prd_cost] is null or [prd_cost] < 0

--data standardization
  
select distinct  [prd_line] FROM [bronze].[crm_prd_info]

--checking for invalid dates
  
select * FROM [bronze].[crm_prd_info]
where [prd_end_dt] < [prd_start_dt]


--cleaning  and loading data from bronze to silver

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
	cat_id		 NVARCHAR(50),
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
	dwh_creat_date		DATETIME2 DEFAULT GETDATE()
);

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates

-- CLEANING & LOADING [bronze].[crm_sales_details] BRONZE TO SILVER

SELECT [sls_ord_num],
[sls_prd_key],
[sls_cust_id],
[sls_order_dt],
[sls_ship_dt],
[sls_due_dt],
[sls_sales],
[sls_quantity],
[sls_price]
FROM [bronze].[crm_sales_details]

--checking for unwanted spaces
SELECT *
FROM [bronze].[crm_sales_details]
WHERE [sls_ord_num] LIKE ' %' 
   OR [sls_ord_num] LIKE '% '

--- checking integrity of the keys

SELECT [sls_ord_num],[sls_prd_key],[sls_cust_id],[sls_order_dt],[sls_ship_dt],
[sls_due_dt],[sls_sales],[sls_quantity],[sls_price]
FROM [bronze].[crm_sales_details]
WHERE [sls_cust_id] NOT IN (SELECT cst_id from silver.[crm_cust_info])
--[sls_prd_key] NOT IN (SELECT prd_key from silver.[crm_prd_info])

-- checking for invalid dates 

select NULLIF([sls_order_dt],0) from [bronze].[crm_sales_details]
where [sls_order_dt] <= 0 OR LEN([sls_order_dt]) <> 8
OR [sls_order_dt] > 20500101 OR [sls_order_dt] < 19000101 

select NULLIF([sls_ship_dt],0) from [bronze].[crm_sales_details]
where [sls_ship_dt] <= 0 OR LEN([sls_ship_dt]) <> 8
OR [sls_ship_dt] > 20500101 OR [sls_ship_dt] < 19000101 

select NULLIF([sls_due_dt],0) from [bronze].[crm_sales_details]
where [sls_due_dt] <= 0 OR LEN([sls_due_dt]) <> 8
OR [sls_due_dt] > 20500101 OR [sls_due_dt] < 19000101 

SELECT * FROM [bronze].[crm_sales_details]
WHERE [sls_order_dt] > [sls_ship_dt] OR [sls_order_dt] > [sls_due_dt]

--CHECKING SALES
--sales = qty* price nd no 0 nd nulls
--sales 0,-ve or null use price qty
--price 0 null use sales and qty
--price-ve make it +ve

select  distinct [sls_sales],
[sls_quantity],
[sls_price]
FROM [bronze].[crm_sales_details]
where  [sls_sales] <> [sls_quantity] * [sls_price]
or [sls_sales] is null or [sls_quantity] is null
or [sls_price]is null
or [sls_sales] <= 0 or [sls_quantity] <= 0
or [sls_price] <= 0
order by [sls_sales],[sls_quantity],[sls_price]


select  distinct [sls_sales],
[sls_quantity],
[sls_price],
case when [sls_sales] is null or [sls_sales] <> [sls_quantity] * abs([sls_price]) or [sls_sales] <=0 
then [sls_quantity] * abs([sls_price])
else [sls_sales] end as  [sls_sales],
case when [sls_price] is null or [sls_price]<= 0 then [sls_sales]/NULLIF([sls_quantity],0)
else [sls_price] END AS [sls_price]

FROM [bronze].[crm_sales_details]
where  [sls_sales] <> [sls_quantity] * [sls_price]
or [sls_sales] is null or [sls_quantity] is null
or [sls_price]is null
or [sls_sales] <= 0 or [sls_quantity] <= 0
or [sls_price] <= 0
order by [sls_sales],[sls_quantity],[sls_price]

--cleaning and loading data from bronze to silver

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_creat_date	DATETIME2 DEFAULT GETDATE()
);
-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
-- CLEANING & LOADING [erp_cust_az12]  BRONZE TO SILVER

SELECT 
[cid],
[bdate],
[gen]
FROM
[bronze].[erp_cust_az12]

--CHECKING FOR KEY 
SELECT 
case when [cid] LIKE 'NAS%'  THEN SUBSTRING([cid],4,LEN([cid]))
ELSE [cid] END AS [cid],
[bdate],
[gen]
FROM
[bronze].[erp_cust_az12]

--CHECKING FOR INVALID DATES
SELECT 
[bdate]
FROM
[bronze].[erp_cust_az12]
WHERE  [bdate] < '1924-01-01' OR [bdate] > GETDATE()

--solution
SELECT 
[bdate],
CASE WHEN  [bdate] > GETDATE() THEN NULL
ELSE [bdate] END AS [bdate1],
DATEDIFF(YEAR,[bdate],GETDATE()) AS YEARS ,
[gen]
FROM
[bronze].[erp_cust_az12]
WHERE  [bdate] < '1924-01-01' OR [bdate] > GETDATE()

--CHECKING GENDER
  
SELECT DISTINCT [gen] FROM
[bronze].[erp_cust_az12]

SELECT DISTINCT 
CASE WHEN UPPER(LTRIM(RTRIM([gen]))) IN ('F','Female') THEN 'Female'
WHEN UPPER(LTRIM(RTRIM([gen]))) IN ('M','Male') THEN 'Male' 
ELSE 'n/a' END AS [gen]
FROM
[bronze].[erp_cust_az12]

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================

-- CLEANING & LOADING [erp_loc_a101]  BRONZE TO SILVER
select [cid],[cntry] from [bronze].[erp_loc_a101]
  
-- Data Standardization & Consistency
--cleaning cid
select cid, replace(cid,'-','') as cid 
from [bronze].[erp_loc_a101]

--cleaning cntry 
select distinct [cntry] from [bronze].[erp_loc_a101]
order by [cntry]

SELECT DISTINCT [cntry],
CASE WHEN (LTRIM(RTRIM([cntry]))) = 'DE' THEN 'Germany'
	 WHEN (LTRIM(RTRIM([cntry]))) IN ('US', 'USA')  THEN 'United States'
	 WHEN (LTRIM(RTRIM([cntry]))) = '' OR (LTRIM(RTRIM([cntry]))) IS NULL THEN 'n/a'
	 ELSE (LTRIM(RTRIM([cntry]))) END AS [cntry]
FROM [bronze].[erp_loc_a101]


-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results

--checking unwanted spaces

SELECT [id],[cat],[subcat],[maintenance] 
FROM [bronze].[erp_px_cat_g1v2]
WHERE [subcat] <> (LTRIM(RTRIM([subcat])))
OR [cat] <> (LTRIM(RTRIM([cat])))
OR [maintenance] <> (LTRIM(RTRIM([maintenance])))

--Data Standardization and consistency
SELECT DISTINCT 
 [maintenance] FROM [bronze].[erp_px_cat_g1v2]

