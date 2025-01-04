SELECT --q4
COUNT(o.order_item_id) AS quantity,
c.product_category_name_english   
FROM 
`ready-de-25.dbt_project_saif.fact_order` o 
LEFT JOIN `ready-de-25.dbt_project_saif.dim_product` p 
ON o.product_id = p.product_id
LEFT JOIN `ready-de-25.dbt_project_saif.dim_category` c 
ON p.product_category_name = c.product_category_name
WHERE 
product_category_name_english IS NOT NULL
GROUP BY  
c.product_category_name_english
ORDER BY 
quantity desc