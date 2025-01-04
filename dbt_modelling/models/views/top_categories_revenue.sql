SELECT --q5
c.product_category_name_english,
SUM(o.price) AS total_revenue
FROM 
`ready-de-25.dbt_project_saif.fact_order` AS o

LEFT JOIN 
`ready-de-25.dbt_project_saif.dim_product` AS p
ON 
 o.product_id = p.product_id

LEFT JOIN `ready-de-25.dbt_project_saif.dim_category` c 
ON p.product_category_name = c.product_category_name

GROUP BY 
 c.product_category_name_english

ORDER BY 
total_revenue DESC


