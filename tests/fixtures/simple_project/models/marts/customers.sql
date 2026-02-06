SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as lifetime_value
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('orders') }} o ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3, 4
