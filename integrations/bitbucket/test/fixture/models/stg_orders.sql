select id as order_id, status
from {{ source('raw', 'orders') }}
