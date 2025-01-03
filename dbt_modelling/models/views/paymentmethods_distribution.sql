SELECT --q10
payment_type,
ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM 
`ready-de-25.dbt_project_saif.fact_payment`
GROUP BY 
payment_type
ORDER BY 
percentage DESC