{{ config(
    materialized='table',
    dist='all',
    sort=['tripid', 'travelstepid'],
) }}

with source as (select * from {{ source('erp_raw', 'ebdb_public_tripdestination') }})

select

    tripid,
    travelstepid,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

from source
