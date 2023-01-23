{{config(materialized="table")}}

with dates as (
    select 
        *
    from raw_dates
)

select * from dates