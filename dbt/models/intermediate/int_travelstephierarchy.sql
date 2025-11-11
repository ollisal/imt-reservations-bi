{{ config(
    materialized='table',
    dist='all',
    sort=['travelstepid'],
) }}

with

travelstep as (select * from {{ ref('stg_travelstep') }}),

-- Find cities (parent of hotels/services)
cities as (
    select
        travelstepid,
        travelstepname as cityname,
        parentid as countryid
    from travelstep
    where travelsteptype = 'City'
),

-- Find countries (parent of cities and bus route groups)
countries as (
    select
        travelstepid,
        travelstepname as countryname
    from travelstep
    where travelsteptype = 'Country'
),

-- Flatten the hierarchy
flattened as (
    select
        ts.travelstepid,
        ts.travelstepname,
        ts.travelsteptype,
        ts.isarchived,

        -- For hotels, check if they use GDS
        case
            when ts.travelsteptype = 'Hotel' then ts.sellhotelfromgds
            else null
        end as isgdshotel,

        -- City information (for hotels, services, and cities themselves)
        case
            when ts.travelsteptype in ('Hotel', 'DuringStayService') then ts.parentid
            when ts.travelsteptype = 'City' then ts.travelstepid
            else null
        end as cityid,

        case
            when ts.travelsteptype in ('Hotel', 'DuringStayService') then c.cityname
            when ts.travelsteptype = 'City' then ts.travelstepname
            else null
        end as cityname,

        -- Country information
        case
            when ts.travelsteptype in ('Hotel', 'DuringStayService') then c.countryid
            when ts.travelsteptype = 'City' then ts.parentid
            when ts.travelsteptype = 'BusRouteGroup' then ts.parentid
            when ts.travelsteptype = 'Country' then ts.travelstepid
            else null
        end as countryid,

        case
            when ts.travelsteptype in ('Hotel', 'DuringStayService') then co.countryname
            when ts.travelsteptype = 'City' then co_direct.countryname
            when ts.travelsteptype = 'BusRouteGroup' then co_direct.countryname
            when ts.travelsteptype = 'Country' then ts.travelstepname
            else null
        end as countryname

    from travelstep as ts
    left join cities as c
        on ts.parentid = c.travelstepid
        and ts.travelsteptype in ('Hotel', 'DuringStayService')
    left join countries as co
        on c.countryid = co.travelstepid
    left join countries as co_direct
        on ts.parentid = co_direct.travelstepid
        and ts.travelsteptype in ('City', 'BusRouteGroup')
)

select
    travelstepid,
    travelstepname,
    travelsteptype,
    isarchived,
    isgdshotel,
    cityid,
    cityname,
    countryid,
    countryname,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

from flattened
