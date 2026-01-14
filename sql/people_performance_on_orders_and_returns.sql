-- Sales Performance by Regional Manager 
SELECT
  p.`Regional Manager` AS Regional_Manager,
  p.Region,
  ROUND(SUM(o.Sales), 2) AS Total_Sales,
  ROUND(SUM(o.Profit), 2) AS Total_Profit,
  COUNT(DISTINCT o.`Order ID`) AS Total_Orders,
  -- Calculate Profit Margin
  ROUND((SUM(o.Profit) / SUM(o.Sales)) * 100, 2) AS Profit_Margin_Pct
FROM
  `verdant-sprite-483914-g4.test.Orders` AS o
INNER JOIN
  `verdant-sprite-483914-g4.test.People` AS p
ON
  o.Region = p.Region
GROUP BY
  1, 2
ORDER BY
  Total_Profit DESC;

-- Check if every order has an assigned manager
SELECT
  o.Region AS Order_Region,
  COUNT(o.`Order ID`) AS Orphaned_Orders
FROM
  `verdant-sprite-483914-g4.test.Orders` AS o
LEFT JOIN
  `verdant-sprite-483914-g4.test.People` AS p
ON
  o.Region = p.Region
WHERE
  p.`Regional Manager` IS NULL
GROUP BY
  1;

-- Check if specific managers rely too heavily on one sales category
SELECT
p.`Regional Manager` AS Regional_Manager,
o.Category,
SUM(o.Sales) AS Category_Sales
FROM
  `verdant-sprite-483914-g4.test.Orders` AS o
JOIN
  `verdant-sprite-483914-g4.test.People` AS p
ON
  o.Region = p.Region
GROUP BY
  1, 2
ORDER BY
  1, 3 DESC;