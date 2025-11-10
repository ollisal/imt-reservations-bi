{{ config(
    materialized='table',
    dist='all',
    sort=['travelstepid'],
) }}

with source as (select * from {{ source('erp_raw', 'ebdb_public_travelstep') }})

select

    id as travelstepid,
    name as travelstepname,
    type as travelsteptype,
    parentid,
    isarchived,
    sellhotelfromgds,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}' as dbt_runid

from source
