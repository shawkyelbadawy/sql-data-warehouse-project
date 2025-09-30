/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,  -- Surrogate Key
	cu.cst_id AS customer_id,
	cu.cst_key AS customer_number,
	cu.cst_firstname AS first_name,
	cu.cst_lastname AS last_name,
	cl.country AS country,
	cu.cst_martial_status AS martial_status,
	CASE WHEN cu.cst_gndr != 'n/a' THEN cu.cst_gndr		-- CRM is the Primary Source for Gender
		ELSE COALESCE(cb.gen, 'n/a')
	END gender,
	cb.bdate AS birthdate,
	cu.cst_create_date AS create_date
FROM silver.crm_cust_info cu
LEFT JOIN silver.erp_cust_az12 cb
	ON cu.cst_key = cb.cid
LEFT JOIN silver.erp_loc_a101 cl
	ON cu.cst_key = cl.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_key) AS product_key,  -- Surrogate Key
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pr.prd_cost AS cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM silver.crm_prd_info pr
INNER JOIN silver.erp_px_cat_g1v2 pc
	ON pr.cat_id = pc.id
WHERE pr.prd_end_dt IS NULL;  -- Filter Out all Historical Data
GO

-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id;
