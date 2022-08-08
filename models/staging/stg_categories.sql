with
    source_data as (
        select
        -- Primary key --
            category_id,
        -- Columns --
            category_name,
            description,
            picture           
        from {{source('northwind_lighthouse','categories')}}
)

select *
from source_data