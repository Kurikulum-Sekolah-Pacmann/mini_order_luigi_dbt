select * 
from {{ source('mini_order', 'customer') }}