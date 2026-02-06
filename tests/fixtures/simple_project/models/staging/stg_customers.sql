SELECT
    id as customer_id,
    first_name,
    last_name,
    email
FROM {{ source('raw', 'customers') }}
