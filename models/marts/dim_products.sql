with
    staging as (
        select *
        from {{ref('stg_products')}}
    )
    , transformed as (
        select
            row_number() over (order by product_id) as product_sk -- auto-incremental surrogate key
            , product_id
            , product_name
            , units_in_stock
            , category_id
            , unit_price
            , quantity_per_unit
            , reorder_level
            , supplier_id
            , units_on_order
            , is_discontinued
        from staging
)

select *  from transformed