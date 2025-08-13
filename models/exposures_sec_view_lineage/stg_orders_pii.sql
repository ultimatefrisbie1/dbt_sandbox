
with orders_pii as 
(select
    *
FROM {{ source('incremental', 'orders_pii') }})

select * from orders_pii