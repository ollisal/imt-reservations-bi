{{ config(
    materialized='table',
    dist='all',
    sort=['tripid', 'productstepindex'],
    sort_type='compound',
) }}

with base as (
    select tripid,
    tripphaseid,
    tripphaseindex,
    tripphasetype

    from {{ ref('stg_tripphase') }}
),

departures as (
    select tripid,
    null as tripphaseid,
    null as tripphaseindex,
    null as tripphasetype,
    'departure' as productsteptype,
    0 as _sortkey

    from base
    group by tripid
),

mainsteps as (
    select tripid,
    tripphaseid,
    tripphaseindex,
    tripphasetype,
    case
        when tripphasetype = 'Flight' then 'flight'
        when tripphasetype = 'Hotel' then 'hotel'
        else 'ship'
        end as productsteptype,
    (tripphaseindex + 1) * (
        case
        when tripphasetype = 'Flight' then 10
        when tripphasetype = 'Hotel' then 1000
        else 1000000
    end) as _sortkey

    from base

    where tripphasetype in ('Flight', 'Hotel', 'Ship')
),

substeps as (
    select tripid,
    tripphaseid,
    tripphaseindex,
    tripphasetype,
    case when tripphasetype = 'Hotel' then 'room' else 'cabin' end as productsteptype,
    (tripphaseindex + 1) * (case when tripphasetype = 'Hotel' then 1000 else 1000000 end) + 1 as _sortkey

    from base

    where tripphasetype in ('Hotel', 'Ship')
),

allsteps as (
    select * from departures
    union all
    select * from mainsteps
    union all
    select * from substeps
),

rankedsteps as (
    select

    tripid,

    (rank() over (partition by tripid order by _sortkey asc)) - 1 as productstepindex,
    productsteptype,

    tripphaseindex,
    tripphaseid,
    tripphasetype

    from allsteps
)

-- TODO create separate set of rows for trip when own car is chosen
-- (although these are usually the same as the default ones,
-- it's typically just choiceless bus etc steps which are skipped)

select *,
(max(productstepindex) over (partition by tripid)) + 1 as numproductsteps

from rankedsteps