{{ config(
    materialized='table',
    diststyle='key',
    distkey='reservationid',
    sort=['departuredate', 'tripid']
) }}

with base as (
    select

    reservationid,

    createdate,
    confirmationdate,
    departuredate,

    tripid,
    trip.name tripname,
    -- Further trip attributes from a separately constructed dim_trip

    status,
    firstundecidedreservationview,
    firstundecidedstepindex

    from {{ ref('stg_reservation') }} r
    join {{ ref('stg_trip') }} trip using (tripid)
),

funnel_staged as (
    select

    base.*,

    case
    when status = 'Confirmed' then 'Confirmed'
    else firstundecidedreservationview
    end finalstage,

    abandonstep.productsteptype abandonproductsteptype,
    abandonstep.numproductsteps totalproductsteps

    from base

    left join {{ ref('int_tripproductstep' )}} abandonstep
    on status != 'Confirmed'
    and firstundecidedreservationview = 'ProductSelection'
    and base.tripid = abandonstep.tripid
    and firstundecidedstepindex = abandonstep.productstepindex
)

select

reservationid,

createdate,
extract(year from createdate) createyear,
extract(month from createdate) createmonth,

confirmationdate,
extract(year from confirmationdate) confirmationyear,
extract(month from confirmationdate) confirmationmonth,

departuredate,
extract(year from departuredate) departureyear,
extract(month from departuredate) departuremonth,

tripid,
tripname,

finalstage,

case when finalstage = 'ProductSelection' then firstundecidedstepindex end as abandonproductstepindex,
abandonproductsteptype,
case when finalstage = 'ProductSelection' then totalproductsteps end as totalproductsteps,

case
    when finalstage = 'Confirmed' then 1.0
    when finalstage = 'Confirmation' then 0.8
    when finalstage = 'AdditionalServices' then 0.75
    when finalstage = 'ReserverInfo' then 0.7
    when finalstage = 'PassengerInfo' then 0.4
    else (abandonproductstepindex::float / totalproductsteps) * 0.4
end as funnel_progress

from funnel_staged