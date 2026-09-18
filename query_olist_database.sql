SELECT 'dim_customers' AS table_name, COUNT(*) AS row_count FROM dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL
SELECT 'fact_orders', COUNT(*) FROM fact_orders
UNION ALL
SELECT 'fact_orders_items', COUNT(*) FROM fact_orders_items
UNION ALL
SELECT 'fact_payments', COUNT(*) FROM fact_payments
UNION ALL
SELECT 'fact_reviews', COUNT(*) FROM fact_reviews;