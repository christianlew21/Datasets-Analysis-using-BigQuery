--Preview Tables
select * from `verdant-sprite-483914-g4.test.People` limit 10;

select * from `verdant-sprite-483914-g4.test.Returns` limit 10;

select * from `verdant-sprite-483914-g4.test.Orders` limit 10;


-- Row Counts for All Tables
SELECT 'orders' as table_name, COUNT(*) as row_count FROM `verdant-sprite-483914-g4.test.Orders`
UNION ALL
SELECT 'people', COUNT(*) FROM `verdant-sprite-483914-g4.test.People`
UNION ALL
SELECT 'returns', COUNT(*) FROM `verdant-sprite-483914-g4.test.Returns`;


-- Check null value on columns of Orders Table
SELECT 
  COUNTIF('Order ID' IS NULL) as null_order_ids,
  COUNTIF('Customer ID' IS NULL) as null_customer_ids,
  COUNTIF('Product ID' IS NULL) as null_product_ids,
FROM `verdant-sprite-483914-g4.test.Orders`;


-- Check for duplicates in primary keys
SELECT `Order ID` as order_id, COUNT(*) as cnt
FROM `verdant-sprite-483914-g4.test.Orders`
GROUP BY order_id
HAVING cnt > 1;


-- Check date formats and ranges
SELECT 
  MIN(`Order Date`) as earliest_order,
  MAX(`Order Date`) as latest_order
FROM `verdant-sprite-483914-g4.test.Orders`;


-- Rename Column Names of People
CREATE OR REPLACE TABLE `verdant-sprite-483914-g4.test.People` AS
SELECT
  -- Renaming the generic fields to original terms
  string_field_0 AS `Regional Manager`,
  string_field_1 AS Region
FROM
  `verdant-sprite-483914-g4.test.People`
WHERE
  string_field_0 != `Regional Manager` 
  AND string_field_0 IS NOT NULL;