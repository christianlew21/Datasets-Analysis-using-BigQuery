  -- Calculate total order, total revenue and average order value per month
  SELECT 
    FORMAT_DATE('%Y-%m', `Order Date`) as month,
    COUNT(*) as order_count,
    SUM(Sales) as monthly_revenue,
    AVG(Sales) as avg_order_value
  FROM `verdant-sprite-483914-g4.test.Orders`
  GROUP BY month
  ORDER BY month;


  -- Calculate total sales per region, and their average order values per region
  SELECT 
    `Region` as Region,
    SUM(`Sales`) as total_sales,
    AVG(`Sales`) as average_order_value
    FROM `verdant-sprite-483914-g4.test.Orders`
  GROUP BY Region
  ORDER BY total_sales DESC;


  -- Calculate total sales per segment, and their average order values per segment
  select
    Segment,
    SUM(`Sales`) as total_sales,
    AVG(`Sales`) as average_order_value
  from 
    `verdant-sprite-483914-g4.test.Orders`
  GROUP BY Segment
  order by total_sales;


  -- Calculate the total costs vs total sales and profit 
  SELECT 
    SUM(Sales) as total_sales,
    SUM(Profit) as total_profit,
    SUM(Sales) - SUM(Profit) as total_costs,
  from
    `verdant-sprite-483914-g4.test.Orders`;


  -- Calculate the average costs, sales and profit per order 
  SELECT 
    AVG(Sales) as average_sales,
    AVG(Profit) as average_profit,
    AVG(Sales) - AVG(Profit) as average_costs,
  FROM
    `verdant-sprite-483914-g4.test.Orders`;


  -- Calculate average order values, costs and profit per Ship mode
  SELECT 
    `Ship Mode` as Ship_Mode,
    AVG(Sales) as average_order_value,
    AVG(Sales) - AVG(Profit) as average_costs_per_order,
    AVG(Profit) as average_profit_per_order
  from
    `verdant-sprite-483914-g4.test.Orders`
  GROUP BY `Ship Mode`