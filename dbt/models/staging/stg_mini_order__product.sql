select *
from {{ source('mini_order', 'product') }}