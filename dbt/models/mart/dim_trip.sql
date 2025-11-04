{{ config(
    materialized='table',
    dist='all',
    sort=['tripid'],
) }}

with trips as (
    select * from {{ ref('stg_trip') }}
),

phases as (
    select * from {{ ref('stg_tripphase') }}
),

trip_with_phases as (
    select
        t.tripid,
        t.name,
        t.allowowncar,
        t.requireowncar,
        t.isarchived,

        p.tripphaseid,
        p.tripphaseindex,
        p.tripphasetype

    from trips t
    left join phases p on t.tripid = p.tripid
),

trip_with_phases_counted as (
    select
        tripid,
        any_value(name) tripname,
        any_value(isarchived) tripisarchived,

        {% for type in var('tripphasetypes') %}
        sum(case when tripphasetype = '{{ type }}' then 1 else 0 end) as tripnum{{ type | lower }}phases,
        {% endfor %}
        count(tripphasetype) tripnumphases,

        current_timestamp as dbt_loadtime,
        '{{ invocation_id }}' as dbt_runid

    from trip_with_phases
    group by tripid
)

-- TODO add destination info

-- TODO create separate row for trip when own car is chosen

select * from trip_with_phases_counted