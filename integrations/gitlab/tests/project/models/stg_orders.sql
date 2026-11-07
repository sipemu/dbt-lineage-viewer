select id, amount from {{ source('raw', 'orders') }}
