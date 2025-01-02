SELECT 
o.order_id,
o.customer_id,
i.product_id,
i.seller_id,
i.order_item_id,
i.price,
i.freight_value,
r.review_score,
o.order_purchase_timestamp,
g.geolocation_zip_code_prefix
FROM `ready-de-25.playground.orders_saif` AS o
LEFT JOIN `ready-de-25.playground.order_items_saif` AS i ON o.order_id = i.order_id 
LEFT JOIN `ready-de-25.playground.sellers_saif` AS s ON i.seller_id = s.seller_id  
LEFT JOIN `ready-de-25.playground.geolocation_saif` AS g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
LEFT JOIN `ready-de-25.playground.order_reviews_saif` AS r ON o.order_id = r.order_id
WHERE i.product_id IS NOT NULL and i.seller_id IS NOT NULL and i.order_item_id IS NOT NULL and i.price IS NOT NULL and i.freight_value IS NOT NULL and g.geolocation_zip_code_prefix IS NOT NULL


        