-- Joined Orders Table on Return Table to view the Return Statuses
SELECT 
  o.`Order ID`,
  o.`Order Date`,
  o.`Ship Mode`,
  o.Sales,
  o.Profit,
  o.Category,
  CASE 
    WHEN r.Returned IS TRUE THEN 'Returned'
    ELSE 'Not Returned'
  END as Return_Status
FROM `verdant-sprite-483914-g4.test.Orders` o
LEFT JOIN `verdant-sprite-483914-g4.test.Returns` r
  ON o.`Order ID` = r.`Order ID`
LIMIT 100;


-- Return Rate Analysis by Ship Mode with Total Sales figures after deducting the returned sales 
SELECT 
  o.`Ship Mode`,
  COUNT(DISTINCT o.`Order ID`) as total_orders,
  COUNT(DISTINCT r.`Order ID`) as returned_orders,
  ROUND(COUNT(DISTINCT r.`Order ID`) * 100.0 / COUNT(DISTINCT o.`Order ID`), 2) as return_rate_pct,
  SUM(o.Sales) as total_sales_before_return,
  SUM(CASE WHEN r.Returned IS TRUE THEN o.Sales ELSE 0 END) as returned_sales,
  SUM(o.Sales) - SUM(CASE WHEN r.Returned IS TRUE THEN o.Sales ELSE 0 END) as total_sales_after_return

FROM `verdant-sprite-483914-g4.test.Orders` o
LEFT JOIN `verdant-sprite-483914-g4.test.Returns` r
  ON o.`Order ID` = r.`Order ID`
GROUP BY o.`Ship Mode`
ORDER BY return_rate_pct DESC;


-- Segment Based Return Rate Analysis
SELECT 
  o.Segment,
  COUNT(DISTINCT o.`Order ID`) as total_orders,
  COUNT(DISTINCT r.`Order ID`) as returned_orders,
  ROUND(COUNT(DISTINCT r.`Order ID`) * 100.0 / COUNT(DISTINCT o.`Order ID`), 2) as return_rate_pct,
  AVG(o.Sales) as avg_order_value,
  AVG(CASE WHEN r.Returned IS TRUE THEN o.Sales END) as avg_returned_order_value
FROM `verdant-sprite-483914-g4.test.Orders` o
LEFT JOIN `verdant-sprite-483914-g4.test.Returns` r
  ON o.`Order ID` = r.`Order ID`
GROUP BY o.Segment
ORDER BY return_rate_pct DESC;


-- Product Category Based Return Rate Analysis
SELECT 
  o.Category,
  COUNT(DISTINCT o.`Order ID`) as total_orders,
  COUNT(DISTINCT r.`Order ID`) as returned_orders,
  ROUND(COUNT(DISTINCT r.`Order ID`) * 100.0 / COUNT(DISTINCT o.`Order ID`), 2) as return_rate_pct,
  SUM(o.Sales) as total_sales,
  SUM(o.Profit) as total_profit,
  SUM(CASE WHEN r.Returned IS TRUE THEN o.Profit ELSE 0 END) as lost_profit_from_returns
FROM `verdant-sprite-483914-g4.test.Orders` o
LEFT JOIN `verdant-sprite-483914-g4.test.Returns` r
  ON o.`Order ID` = r.`Order ID`
GROUP BY o.Category
ORDER BY return_rate_pct DESC;


-- Profitability Impact of Returns on Product Category
SELECT 
  o.Category,
  SUM(o.Sales) as total_sales,
  SUM(o.Profit) as total_profit,
  SUM(CASE WHEN r.Returned IS TRUE THEN o.Sales ELSE 0 END) as returned_sales,
  SUM(CASE WHEN r.Returned IS TRUE THEN o.Profit ELSE 0 END) as lost_profit_due_to_return,
  ROUND(SUM(CASE WHEN r.Returned IS TRUE THEN o.Profit ELSE 0 END) * 100.0 / 
        NULLIF(SUM(o.Profit), 0), 2) as profit_loss_pct
FROM `verdant-sprite-483914-g4.test.Orders` o
LEFT JOIN `verdant-sprite-483914-g4.test.Returns` r
  ON o.`Order ID` = r.`Order ID`
GROUP BY o.Category
ORDER BY lost_profit_due_to_return DESC;


-- High-Value Returns
SELECT 
  o.`Order ID`,
  o.Sales,
  o.Profit,
  o.`Order Date`,
  o.`Ship Mode`,
  r.Returned
FROM `verdant-sprite-483914-g4.test.Orders` o
INNER JOIN `verdant-sprite-483914-g4.test.Returns` r
  ON o.`Order ID` = r.`Order ID`
WHERE r.Returned IS TRUE
  AND o.Sales > 1000  -- High-value orders
ORDER BY o.Sales DESC;