select * 
from {{ source('mini_order', 'category') }}