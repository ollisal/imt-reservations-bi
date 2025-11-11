{{ config(
    materialized='incremental',
    unique_key='reservationid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    dist='reservationid',
    sort=['departuredate', 'createtime'],
    sort_type='compound',
) }}

-- Customers are Finnish so the reservation lifecycle events are considered to be in Helsinki timezone, but the timezone
-- information seems to have been lost in the data lake
{% set home_tz = 'Europe/Helsinki' %}

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
    convert_timezone('{{ home_tz }}', {{ event }}time) at time zone '{{ home_tz }}' as {{ event }}time,
    trunc(convert_timezone('{{ home_tz }}', {{ event }}time)) as {{ event }}date,
    {% endfor %}

    firstundecidedreservationview,
    firstundecidedstepindex,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

-- TODO something from tripsettings? for duration etc.... it's typed as string now!
from source

where
    departuredate is not null  -- not completely "null"
    and mainreservationid is null  -- disregard add-on reservations (mobile, reserve extra service after)
