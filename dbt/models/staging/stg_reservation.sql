{{ config(
    materialized='incremental',
    unique_key='reservationid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    dist='reservationid',
    sort=['departuredate', 'createtime'],
    sort_type='compound',
) }}

with
    source as (
        select *
        from {{ source('erp_raw', 'ebdb_public_reservation') }}
        {% if is_incremental() %}
            where modifytime > coalesce((select max(modifytime) from {{ this }}), '1989-06-28'::timestamp)
        {% endif %}
    )

select

    id as reservationid,
    status,

    tripid,

    (creatoruserid is null) as fromwebshop,
    customerpersonid as customerpersonid,

    departuredate,

    {% for event in ['create', 'confirmation', 'modify'] %}
        {% set timestampcol = event ~ 'time' %}
        {{ datalake_hometime_to_timestamptz(timestampcol) }} as {{ timestampcol }},
        trunc({{ datalake_hometime_to_timestamp(timestampcol) }}) as {{ event }}date,
    {% endfor %}

    firstundecidedreservationview,
    firstundecidedstepindex,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

-- TODO something from tripsettings? for duration etc.... it's typed as string now!
from source

where
    departuredate is not null and tripid is not null  -- not completely "blank"
    and mainreservationid is null  -- disregard add-on reservations (mobile, reserve extra service after)
