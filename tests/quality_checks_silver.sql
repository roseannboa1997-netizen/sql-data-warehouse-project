/*
==============================================================================
Quality Checks
==============================================================================
Script Purpose:
    This script performs various quality checks for data consistensy, accuracy
    and standardization across the 'silver' schema. It includes check for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date range and orders.
    - Data consistensy between related fields

Usage Notes: 
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancy found during the checks.
=============================================================================
*/

--Check for NULLS or Duplicates in Primary Key
--Expectation: No Result

--==================================
--Checking siver.crm_cust_info
--==================================
SELECT 
cst_id,
COUNT (*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

--Check for Unwanted Spaces in string values
--Expectation: No Result
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

--Data standadization & consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info


--==================================
--Checking siver.crm_prd_info
--==================================
--Check for NULLS or Duplicates in Primary Key
--Expectation: No Result
SELECT
prd_id,
COUNT (*) 
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--Check for Unwanted Spaces in string values
--Expectation: No Result
SELECT
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) 

--Data standadization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--Check for NULLS or Negative Numbers
--Expectation: No Results
Select 
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


--==================================
--Checking siver.crm_sales_details
--==================================
--Check for Invalid Date 
SELECT 
NULLIF(sls_order_dt,0) sls_order_dr,
sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101
OR sls_order_dt > sls_due_dt

--Check for Invalid Date Order
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Check Data Consistensy: Between Sales, Quantity, and Price
-->> Sales = Quantity * Price
-->> Values must not be NULL, zero or negative
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales is NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales,sls_quantity,sls_price


--==================================
--Checking silver.erp_cust_az12
--==================================
--
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' 
	 THEN SUBSTRING(cid,4, LEN(cid))
ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12
WHERE gen = 'n/a'
WHERE CASE WHEN cid LIKE 'NAS%' 
	 THEN SUBSTRING(cid,4, LEN(cid))
ELSE cid
END IN (Select cst_key from silver.crm_cust_info)

--Identify out-of-range dates 
SELECT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization and Consistensy
SELECT DISTINCT gen
FROM silver.erp_cust_az12


--==================================
--Checking silver.erp_loc_a101
--==================================
SELECT
REPLACE (cid, '-', '') cid
FROM silver.erp_loc_a101

--Data Standardization and Consistensy
SELECT DISTINCT cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END cntry 
FROM silver.erp_loc_a101

--==================================
--Checking silver.erp_px_cat_g1v2
--==================================
SELECT 
id,
cat,
subcat,
maintenance
FROM erp_px_cat_g1v2

-- Check for unwanted spaces
SELECT
*
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

--Data Standardization and Consistensy
SELECT
DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2
