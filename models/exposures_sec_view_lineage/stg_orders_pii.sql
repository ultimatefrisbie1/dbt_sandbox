select
    *
FROM {{ source('incremental', 'orders_pii') }}