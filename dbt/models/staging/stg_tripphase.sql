{{ config(
    materialized='table',
    dist='all',
    sort=['tripid', 'tripphaseindex'],
    sort_type='compound',
) }}

select
    id as tripphaseid,
    tripid,
    index as tripphaseindex,
    type as tripphasetype,
    domesticbustophase,
    domesticbusfromphase,
    skipwhenowncar,
    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}' as dbt_runid

from {{ source('erp_raw', 'ebdb_public_tripphase') }}
