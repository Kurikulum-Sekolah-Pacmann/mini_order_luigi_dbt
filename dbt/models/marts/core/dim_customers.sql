with stg_dim_customers as (
    select 
        customer_id as nk_customer_id,
        first_name,
        last_name, 
        concat(first_name, ' ', last_name) as full_name,
        email,
        phone,
        address
    from {{ ref ("stg_mini_order__customer") }}
),

final_dim_customers as (
    select
        {{ dbt_utils.generate_surrogate_key( ["nk_customer_id"] ) }} as sk_customer_id,
        *,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_dim_customers
)

select * from final_dim_customers