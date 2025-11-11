{{ config(
    materialized='table',
    dist='all',
    sort=['tripid'],
) }}

with source as (select * from {{ source('erp_raw', 'ebdb_public_triptype') }})

select

    tripid,
    type,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

from source
