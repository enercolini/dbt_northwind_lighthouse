with
    staging as (
        select *
        from {{ref('stg_suppliers') }}
    )
    , transformed as (
        select
            row_number() over (order by supplier_id) as supplier_sk -- auto-incremental surrogate key
            , supplier_id
            , country
            , city
            , postal_code
            , address
            , contact_name
            , phone
            , company_name
            , contact_title
        from staging
    )

select *  from transformed