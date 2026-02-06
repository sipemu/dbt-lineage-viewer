SELECT
    id as payment_id,
    order_id,
    amount,
    payment_method
FROM {{ source('raw', 'payments') }}
