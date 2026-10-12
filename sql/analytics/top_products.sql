-- Business question:
-- Which products have the highest unit sales?
-- Exclude cancelled orders from this analysis.

SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(i.quantity) AS units_sold,
    ROUND(SUM(i.quantity * i.unit_price), 2) AS sales_value
FROM order_items i
JOIN products p
    ON i.product_id = p.product_id
JOIN orders o
    ON i.order_id = o.order_id
WHERE o.status <> 'cancelled'
GROUP BY
    p.product_id,
    p.product_name,
    p.category
ORDER BY
    units_sold DESC,
    sales_value DESC
LIMIT 10;
