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
-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 
	OR cst_id IS NULL;


-- Check For Unwanted Spaces
-- Expectation: No Result

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- View The Table

SELECT * 
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 
	OR prd_id IS NULL;

-- Check For Unwanted Spaces
-- Expectation: No Result

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Data Standardization & Consistency

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results

SELECT prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 
	OR prd_cost IS NULL;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- View The Table

SELECT * 
FROM silver.crm_prd_info;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check For Invalid Dates
-- Expectation: No Results

SELECT NULLIF(sls_order_dt, 0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0
	OR LEN(sls_order_dt) != 8
	OR sls_order_dt > 20500101
	OR sls_order_dt < 19000101;

-- Check For Invalid Date Orders (Order Date > Shipping/Due Dates, Shipping Dates > Due Dates)
-- Expectation: No Results

SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
	OR sls_order_dt > sls_due_dt
	OR sls_ship_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results

SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- View The Table

SELECT * 
FROM silver.crm_sales_details;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today

SELECT DISTINCT bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency

SELECT DISTINCT gen 
FROM silver.erp_cust_az12;

-- View The Table

SELECT * 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency

SELECT DISTINCT country
FROM silver.erp_loc_a101;

-- View The Table

SELECT * 
FROM silver.erp_loc_a101;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results

SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
	OR subcat != TRIM(subcat) 
	OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency

SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2;

-- View The Table

SELECT * 
FROM silver.erp_px_cat_g1v2;