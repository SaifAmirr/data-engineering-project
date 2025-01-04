SELECT --q7
FORMAT_TIMESTAMP('%Y-%m', order_purchase_timestamp) AS order_month,
COUNT(order_id) AS total_orders
FROM 
    `ready-de-25.dbt_project_saif.fact_order`
GROUP BY 
    order_month
ORDER BY 
    order_month DESC