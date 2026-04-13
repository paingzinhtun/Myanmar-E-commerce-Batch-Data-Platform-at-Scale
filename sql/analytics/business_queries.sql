SELECT r.region,
       r.city,
       SUM(o.order_total_mmk) AS gmv_mmk,
       COUNT(DISTINCT o.order_id) AS total_orders
FROM analytics.fact_orders o
LEFT JOIN analytics.dim_region r ON o.region_key = r.region_key
GROUP BY r.region, r.city
ORDER BY gmv_mmk DESC;

SELECT oi.category,
       p.product_name,
       SUM(oi.net_line_amount_mmk) AS revenue_mmk,
       SUM(oi.quantity) AS units_sold
FROM analytics.fact_order_items oi
JOIN analytics.dim_product p ON oi.product_id = p.product_id
GROUP BY oi.category, p.product_name
ORDER BY revenue_mmk DESC;

SELECT s.seller_name,
       s.region,
       SUM(oi.net_line_amount_mmk) AS seller_revenue_mmk
FROM analytics.fact_order_items oi
JOIN analytics.dim_seller s ON oi.seller_id = s.seller_id
GROUP BY s.seller_name, s.region
ORDER BY seller_revenue_mmk DESC;

SELECT d.year,
       d.month,
       o.order_date,
       COUNT(DISTINCT o.order_id) AS total_orders,
       SUM(o.order_total_mmk) AS gmv_mmk
FROM analytics.fact_orders o
JOIN analytics.dim_date d
  ON TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER = d.date_key
GROUP BY d.year, d.month, o.order_date
ORDER BY o.order_date;

SELECT region,
       AVG(delivery_delay_days) AS avg_delay_days,
       SUM(CASE WHEN delivery_delay_days <= 0 THEN 1 ELSE 0 END) AS on_time_deliveries
FROM analytics.fact_delivery_performance
GROUP BY region
ORDER BY avg_delay_days;

SELECT p.product_name,
       i.snapshot_date,
       i.available_quantity,
       i.inventory_status
FROM analytics.fact_inventory_snapshot i
JOIN analytics.dim_product p ON i.product_id = p.product_id
WHERE i.inventory_status = 'out_of_stock'
ORDER BY i.snapshot_date DESC, p.product_name;

SELECT c.customer_name,
       c.city,
       SUM(o.order_total_mmk) AS lifetime_value_mmk
FROM analytics.fact_orders o
JOIN analytics.dim_customer c ON o.customer_id = c.customer_id
GROUP BY c.customer_name, c.city
ORDER BY lifetime_value_mmk DESC
LIMIT 20;

SELECT r.region,
       o.payment_method,
       COUNT(*) AS orders_count,
       SUM(o.order_total_mmk) AS payment_volume_mmk
FROM analytics.fact_orders o
LEFT JOIN analytics.dim_region r ON o.region_key = r.region_key
GROUP BY r.region, o.payment_method
ORDER BY r.region, payment_volume_mmk DESC;
