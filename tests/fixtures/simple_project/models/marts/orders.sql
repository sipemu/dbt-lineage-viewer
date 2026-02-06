SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    p.amount as total_amount,
    p.payment_method
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
