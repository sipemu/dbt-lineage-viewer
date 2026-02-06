SELECT *
FROM {{ ref('orders') }}
WHERE total_amount < 0
