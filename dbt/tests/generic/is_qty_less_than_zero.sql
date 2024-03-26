{% test is_qty_less_than_zero(model, column_name) %}

with quantity as (

    select {{ column_name }} as quantity_data
    from {{ model }}

),

check_quantity as (
    select
        quantity_data
    from
        quantity
    where
        quantity_data < 0 
)

select * from check_quantity

{% endtest %}