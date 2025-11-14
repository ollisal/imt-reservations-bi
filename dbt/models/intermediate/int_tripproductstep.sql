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

flightsteps as (
    select tripid,
    tripphaseid,
    tripphaseindex,
    tripphasetype,
    'flight' as productsteptype,
    (tripphaseindex + 1) * 10 as _sortkey

    from base

    {# Both flight directions are chosen in one step, there is no additional step like there would be for return ship #}
    where tripphasetype = 'Flight'
    and not exists (
        select 1 from base tp2
        where tp2.tripid = base.tripid
        and tp2.tripphaseindex > base.tripphaseindex
        and tp2.tripphasetype = 'Flight'
    )
),

mainsteps as (
    select tripid,
    tripphaseid,
    tripphaseindex,
    tripphasetype,
    case
        when tripphasetype = 'Hotel' then 'hotel'
        else 'ship'
        end as productsteptype,
    (tripphaseindex + 1) * (
        case
        when tripphasetype = 'Hotel' then 1000
        else 1000000
    end) as _sortkey

    from base

    where tripphasetype in ('Hotel', 'Ship')
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
    select * from flightsteps
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
(max(productstepindex) over (partition by tripid)) + 1 as numproductsteps,

current_timestamp as dbt_loadtime,
'{{ invocation_id }}'::text as dbt_runid

from rankedsteps