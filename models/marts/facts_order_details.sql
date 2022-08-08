with
    customers as (
        select
        customer_sk
        , customer_id
        from {{ref('dim_customers')}}
    )
    , employees as (
        select
        employee_sk
        , employee_id
        from {{ref('dim_employees')}}
    )
    , suppliers as (
        select
        supplier_sk
        , supplier_id
        from {{ref('dim_suppliers')}}
    )
    , shippers as (
        select
          shipper_sk
        , shipper_id
        from {{ref('dim_shippers')}}
    )
    , products as (
        select
        product_sk
        , product_id
        , category_id
        from {{ref('dim_products')}}
    )
    , categories as (
        select
        category_sk
        , category_id
        from {{ref('dim_categories')}}
    )
    , orders_with_sk as (
        select
            orders.order_id
            , employees.employee_sk as employee_fk
            , customers.customer_sk as customer_fk
            , shippers.shipper_sk as shipper_fk
            , orders.order_date
            , orders.ship_region
            , orders.shipped_date
            , orders.ship_country
            , orders.ship_name
            , orders.ship_postal_code
            , orders.ship_city
            , orders.freight
            , orders.ship_address
            , orders.required_date
        from {{ref('stg_orders')}} orders
        left join employees employees on orders.employee_id = employees.employee_id
        left join customers customers on orders.customer_id = customers.customer_id
        left join shippers shippers on orders.shipper_id = shippers.shipper_sk
    )
    , order_details_with_sk as (
        select
            order_dtl.order_id
            , products.product_sk as product_fk
            , categories.category_sk as category_fk
            , order_dtl.discount
            , order_dtl.unit_price
            , order_dtl.quantity
        from {{ref('stg_order_details')}} order_dtl
        left join products products on order_dtl.product_id = products.product_id
        left join categories categories on products.category_id = categories.category_id 
    )

    /* We then join orders and order details to get the final fact table*/
    , final as (
        select
        order_dtl.order_id
        , orders.employee_fk
        , orders.customer_fk
        , orders.shipper_fk
        , orders.order_date
        , orders.ship_region
        , orders.shipped_date
        , orders.ship_country
        , orders.ship_name
        , orders.ship_postal_code
        , orders.ship_city
        , orders.freight
        , orders.ship_address
        , orders.required_date
        , order_dtl.product_fk
        , order_dtl.discount
        , order_dtl.unit_price
        , order_dtl.quantity
        from orders_with_sk orders
        left join order_details_with_sk order_dtl on orders.order_id = order_dtl.order_id
    )

select * from final
