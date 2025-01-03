SELECT --q1
customer_id, 
sum(price) as total_order_value 
FROM 
`ready-de-25.dbt_project_saif.fact_order` 
group by 
customer_id
order by 
total_order_value desc

