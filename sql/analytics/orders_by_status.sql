-- Business question:
-- How many orders and how much order value exist per status?

SELECT
    status,
    COUNT(*) AS total_orders,
    ROUND(SUM(total_amount), 2) AS order_value,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM orders
GROUP BY status
ORDER BY total_orders DESC;
