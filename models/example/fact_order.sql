{{ config(materialized="table")}}

with
    customer as (select * from {{ref("dim_customer")}}),
    product as (select * from {{ref("dim_product")}}),
    stores as (select * from {{ref("dim_stores")}}),
    orders as (

        select 
            customer_id,
            b.product_id,
            sum(quantity * b.price) as amount,
            sum(quantity) as quantity,
            count(a.order_id) as number_of_order
        from
            raw_order as a
        left join raw_order_items b on (a.order_id = b.order_id)
        left join product d on (b.product_id = d.product_id)
        group by customer_id, b.product_id

    ),
    final as (
        select * from orders
    )

select * from final