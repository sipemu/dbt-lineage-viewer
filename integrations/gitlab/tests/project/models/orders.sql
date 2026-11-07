select id, sum(amount) as amount from {{ ref('stg_orders') }} group by id
