{{ config(
    materialized='table',
    dist='all',
    sort=['tripdestinationid'],
) }}

with source as (select * from {{ source('erp_raw', 'ebdb_public_tripdestination') }})

select

    id as tripdestinationid,
    tripid,
    travelstepid,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}' as dbt_runid

from source
