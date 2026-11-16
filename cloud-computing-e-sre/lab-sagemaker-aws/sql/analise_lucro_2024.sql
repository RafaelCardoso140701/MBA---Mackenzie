-- Lucro por região em 2024: região com o menor lucro total
SELECT
    region,
    SUM(total_profit) AS "total_profit"
FROM "AwsDataCatalog"."customer_insights_db"."sales_data"
WHERE YEAR(order_date) = 2024
GROUP BY region
ORDER BY "total_profit" ASC
LIMIT 1;

-- Lucro por país em 2024: cinco países com o menor lucro total
SELECT
    country,
    SUM(total_profit) AS "total_profit"
FROM "AwsDataCatalog"."customer_insights_db"."sales_data"
WHERE YEAR(order_date) = 2024
GROUP BY country
ORDER BY "total_profit" ASC
LIMIT 5;
