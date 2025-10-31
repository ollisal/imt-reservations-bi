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

    createtime,
    modifytime,
    confirmationtime,

    date_trunc('day', createtime) as createdate,
    date_trunc('day', modifytime) as modifydate,
    date_trunc('day', confirmationtime) as confirmationdate,

    firstundecidedreservationview,
    firstundecidedstepindex,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}' as dbt_runid

-- TODO something from tripsettings? for duration etc.... it's typed as string now!
from source

where
    departuredate is not null  -- not completely "null"
    and mainreservationid is null  -- disregard add-on reservations (mobile, reserve extra service after)
