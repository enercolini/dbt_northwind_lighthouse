with
    source_data as (
        select
        -- Primary key --
            order_id,
        -- Columns --
            product_id,
            discount,
            unit_price,
            quantity,
        from {{source('northwind_lighthouse','order_details')}}
    )

select *
from source_data