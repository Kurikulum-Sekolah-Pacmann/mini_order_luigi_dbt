with stg_dim_products as (
    select 
        product_id as nk_product_id,
        "name",
        subcategory_id,
        price,
        stock
    from {{ ref("stg_mini_order__product") }}
),

stg_subcategory as (
    select * 
    from {{ ref("stg_mini_order__subcategory") }}
),

stg_category as (
    select *
    from {{ ref("stg_mini_order__category") }}
),

final_dim_products as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["nk_product_id"] ) }} as sk_product_id,
        sdp.nk_product_id,
        sdp."name",
        sdp.price,
        sdp.stock,
        sc."name" as category_name,
        sc.description as category_desc,
        ssc."name" as subcategory_name,
        ssc.description as subcategory_desc,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_dim_products sdp
    inner join stg_subcategory ssc
        on ssc.subcategory_id = sdp.subcategory_id
    inner join stg_category sc
        on sc.category_id = ssc.category_id
)

select * from final_dim_products