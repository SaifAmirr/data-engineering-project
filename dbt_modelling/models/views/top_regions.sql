SELECT 
g.geolocation_state,
COUNT(o.order_id) AS total_orders
FROM `ready-de-25.dbt_project_saif.fact_order` o
LEFT JOIN `ready-de-25.dbt_project_saif.dim_geolocation` g 
ON o.geolocation_zip_code_prefix = g.geolocation_zip_code_prefix
GROUP BY g.geolocation_state
ORDER BY total_orders DESC