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
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results



-- Check if there are duplicates rows

SELECT
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Flag the rows with 1 being the most recent, find duplicate rows

SELECT
*
FROM (
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last != 1


SELECT
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted space

SELECT
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT
	cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

SELECT
	prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

SELECT
*
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)

-- Check all characters for gender
-- Expectation: no null, use full words instead of abrievations

SELECT DISTINCT
	cst_gndr
FROM bronze.crm_cust_info

-- Check all characters for marital status
-- Expectation: no null, use full words instead of abrievations

SELECT DISTINCT
	cst_marital_status
FROM bronze.crm_cust_info

SELECT DISTINCT
	prd_line
FROM bronze.crm_prd_info

SELECT DISTINCT
	gen
FROM bronze.erp_cust_az12

SELECT DISTINCT
	cntry
FROM bronze.erp_loc_a101

SELECT DISTINCT
	cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
	subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2

-- check for NULLS or negative numbers

SELECT
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- check for invalid date orders
SELECT * 
FROM Bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <=0
-- already checked that only sls_order_dt has 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20260101 
OR sls_order_dt < 19990101

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > '2026-01-01'

-- check for price

SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_sales <= 0
OR sls_quantity IS NULL OR sls_quantity <= 0
OR sls_price IS NULL OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price
-- sls_quantity seems fine
