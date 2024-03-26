select *
from {{ source('mini_order', 'orders') }}