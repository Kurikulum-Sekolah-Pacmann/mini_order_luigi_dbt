select * 
from {{ source('mini_order', 'order_detail') }}