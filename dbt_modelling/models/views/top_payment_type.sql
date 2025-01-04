SELECT 
p.payment_type,
SUM(o.price) AS total_revenue
FROM 
`ready-de-25.dbt_project_saif.fact_order` AS o 

LEFT JOIN 
`ready-de-25.dbt_project_saif.fact_payment` AS p 
ON 
o.order_id = p.order_id
WHERE payment_type IS NOT NULL
GROUP BY 
p.payment_type
ORDER BY 
total_revenue DESC