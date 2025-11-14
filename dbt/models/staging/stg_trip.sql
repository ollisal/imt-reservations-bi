{{ config(
    materialized='table',
    dist='all',
    sort=['tripid'],
) }}

with source as (select * from {{ source('erp_raw', 'ebdb_public_trip') }})

select

    id as tripid,
    name,
    primarydestinationid,
    primarytype,
    tripduration,
    allowowncar,
    requireowncar,
    isarchived,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

    {# TODO import cancellationprotectiondisabled, we need it to correctly interpret new reservations Nov 2025 -> which may have protection step before PassengerInfo #}

from source
