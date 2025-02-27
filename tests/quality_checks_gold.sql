/*Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer.
*/
--checking quality of view dim_customers
SELECT DISTINCT
	ci.[cst_gndr],
	ca.gen,
	CASE WHEN ci.[cst_gndr] <> 'n/a' THEN ci.[cst_gndr]
	ELSE COALESCE (ca.gen ,'n/a') end as new_gen
FROM [silver].[crm_cust_info] ci
LEFT JOIN silver.[erp_cust_az12] ca
		ON  ci.[cst_key] = ca.cid
LEFT JOIN silver.[erp_loc_a101] la
	     ON  ci.[cst_key] = la.cid

select * from [gold].[dim_customers]

select distinct gender from [gold].[dim_customers]

--checking quality of view dim_product
 SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

--checking quality of view fact_sales
select * from [gold].[fact_sales]

select * from [gold].[fact_sales] sd
LEFT JOIN [gold].[dim_customers] cu
ON		sd.customer_key = cu.customer_key
where cu.customer_key is null

select * from [gold].[fact_sales] sd
LEFT JOIN  gold.dim_products pr
ON      sd.product_key = pr.product_key
where pr.product_key is null
