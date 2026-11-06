select * from {{ ref('dim_customers', v=1) }}
