{{ config(
    materialized='table',
    dist='reservationid',
    sort=['departuredate', 'tripid'],
    sort_type='compound',
) }}
-- TODO incremental materialization?

with base as (
    select

    reservationid,

    createdate,
    confirmationdate,
    departuredate,

    tripid,
    trip.name tripname,
    -- TODO include choice of own car. It is specified in the first passenger
    -- Further trip attributes from a separately constructed dim_trip

    status,
    firstundecidedreservationview,
    firstundecidedstepindex

    from {{ ref('stg_reservation') }} r
    join {{ ref('stg_trip') }} trip using (tripid)

    where createdate >= '2017-01-01'::date -- firstundecidedreservationview wasn't properly populated earlier in 2016
),

reconfigured_trips as (
    select *, 'unknown'::varchar as productsteptype from {{ ref('ref_reconfigured_trips') }}
),

funnel_staged as (
    select

    base.*,

    case
        when status = 'Confirmed' then 'Confirmed'
        -- It seems for a few reservations the bump from ProductSelection -> PassengerInfo hasn't gone through although step index has been incremented (separate XHR requests)
        when firstundecidedreservationview = 'ProductSelection' and firstundecidedstepindex = (
            select numproductsteps from {{ ref('int_tripproductstep' )}} totalsteps
            where base.tripid = totalsteps.tripid
            and totalsteps.productstepindex = 0
        ) then 'PassengerInfo'
        else firstundecidedreservationview
    end finalstage,

    case
        when finalstage = 'ProductSelection'
            then coalesce(rt.productsteptype, abandonstep.productsteptype)
        else null
    end abandonproductsteptype,
    abandonstep.numproductsteps totalproductsteps

    from base

    left join {{ ref('int_tripproductstep' )}} abandonstep
    on status != 'Confirmed'
    and firstundecidedreservationview = 'ProductSelection'
    and base.tripid = abandonstep.tripid
    and firstundecidedstepindex = abandonstep.productstepindex

    -- don't claim we understand product selection for since (probably) reconfigured trips
    left join reconfigured_trips rt
    on base.departuredate < rt.stable_since
    and firstundecidedreservationview = 'ProductSelection'
    and (
        -- Known reconfiguration
        base.tripid = rt.tripid
        or
        -- Wildcard for so old reservations which we don't bother to research still being valid
        base.tripid not in (select tripid from reconfigured_trips where tripid is not null)
        and rt.tripid is null
    )
)

select

reservationid,

{% for event in ['create', 'confirmation', 'departure'] %}

{{ event }}date,
extract(year from {{ event }}date) {{ event }}year,
extract(month from {{ event }}date) {{ event }}month,

{% endfor %}

tripid,
tripname,

finalstage,

case when finalstage = 'ProductSelection' then firstundecidedstepindex end as abandonproductstepindex,
abandonproductsteptype,
case when finalstage = 'ProductSelection' then totalproductsteps end as totalproductsteps,

case
    when finalstage = 'Confirmed' then 1.0
    when finalstage = 'Confirmation' then 0.8
    {# FIXME: For very recent reservations (Early Nov 2025->?) AdditionalServices is actually before PassengerInfo. Not all trips have it available though, e.g. no trips with Flights #}
    when finalstage = 'AdditionalServices' then 0.75
    when finalstage = 'ReserverInfo' then 0.7
    when finalstage = 'PassengerInfo' then 0.4
    else (abandonproductstepindex::float / totalproductsteps) * 0.4
end as funnel_progress,

funnel_progress >= 0.4 reached_passengerinfo,
funnel_progress >= 0.7 reached_reserverinfo,
funnel_progress >= 0.75 reached_additionalservices,
funnel_progress >= 0.8 reached_confirmation,
finalstage = 'Confirmed' is_confirmed,

current_timestamp as dbt_loadtime,
'{{ invocation_id }}'::text as dbt_runid

from funnel_staged