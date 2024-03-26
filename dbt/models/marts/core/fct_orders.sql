with stg_fct_orders as (
    select
        order_id as nk_order_id,
        customer_id as nk_customer_id,
        order_date::date as order_date,
        "status"
    from {{ ref("stg_mini_order__orders") }}
),

stg_order_details as (
    select 
        order_detail_id as nk_order_detail_id,
        order_id as nk_order_id,
        product_id as nk_product_id,
        quantity,
        price
    from {{ ref("stg_mini_order__order_detail") }}
),

dim_products as (
    select *
    from {{ ref("dim_products") }}
),

dim_customers as (
    select *
    from {{ ref("dim_customers") }}
),

dim_dates as (
    select *
    from {{ ref("dim_dates") }}
),

final_fct_orders as (
    select
        {{ dbt_utils.generate_surrogate_key( ["sfo.nk_order_id"] ) }} as sk_order_id,
        sfo.nk_order_id,
        dp.sk_product_id,
        dc.sk_customer_id,
        dd.date_day as order_date,
        sod.quantity as order_qty,
        sfo.status,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_order_details sod
    inner join stg_fct_orders sfo
        on sfo.nk_order_id = sod.nk_order_id
    inner join dim_products dp 
        on dp.nk_product_id = sod.nk_product_id
    inner join dim_customers dc 
        on dc.nk_customer_id = sfo.nk_customer_id
    inner join dim_dates dd 
        on dd.date_day = sfo.order_date
)

select * from final_fct_orders