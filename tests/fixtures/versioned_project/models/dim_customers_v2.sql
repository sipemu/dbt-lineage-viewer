select id, name from {{ ref('stg_customers') }}
