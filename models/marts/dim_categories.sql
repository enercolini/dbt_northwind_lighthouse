with 
    staging as (
        select *
        from {{ref('stg_categories')}}
    )
    , transformed as (
        select
        -- Surrogate key --
            row_number() over (order by category_id) as category_sk -- auto-incremental surrogate key
        -- Natural key --
            , category_id
        -- Columns --
            , category_name
            , description
            , picture           
        from staging
    )

    select *  from transformed