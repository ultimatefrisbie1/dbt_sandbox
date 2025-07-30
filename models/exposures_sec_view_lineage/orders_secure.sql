-- semantic ref for lineage continuity
-- {{ ref('stg_orders_pii') }}

{{
    config(
        materialized='no_materialization'
    )
}}
 
select
    *
from {{target.database}}.{{target.schema}}.orders_secure