{{ config(
    materialized='incremental',
    unique_key='custKey',
    incremental_strategy='merge'


    ) }}

with source_data as (

    select * FROM {{ source('incremental', 'incremental_raw') }}
)

select * FROM source_data