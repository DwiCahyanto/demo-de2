{{config(materialized="table")}}

with product as (
    select 
        *
    from raw_products
)

select * from product