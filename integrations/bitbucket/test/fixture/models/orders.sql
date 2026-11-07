select order_id, status
from {{ ref('stg_orders') }}
