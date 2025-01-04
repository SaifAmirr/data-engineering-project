SELECT 
ctg.product_category_name_english,
ROUND(AVG(o.review_score), 2) AS average_review_score
FROM
`ready-de-25.dbt_project_saif.fact_order` o

LEFT JOIN `ready-de-25.dbt_project_saif.dim_product` p
ON o.product_id = p.product_id

LEFT JOIN `ready-de-25.dbt_project_saif.dim_category` ctg
ON p.product_category_name = ctg.product_category_name

WHERE ctg.product_category_name_english IS NOT NULL

GROUP BY ctg.product_category_name_english
ORDER BY average_review_score DESC