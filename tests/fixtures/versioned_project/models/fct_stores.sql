select * from {{ ref('stores') }} union all select * from {{ ref('stores', version=2) }}
