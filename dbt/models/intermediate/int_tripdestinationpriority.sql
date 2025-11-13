{{config(
    materialized='view',
)}}

{# If a trip doesn't have primarydestinationid, we use the destination at the most granular level with just a single destination #}
{% set trip_destination_granularities={
    'DuringStayService': 31,
    'Hotel': 30,
    'City': 20,
    'Country': 11,
    'Ship': 10,
    'BusRouteGroup': -1
} %}

with trip as (
    select
        tripid,
        primarydestinationid

    from {{ ref('stg_trip') }}
),

tripdestination_ts as (
    select
        td.tripid,
        td.travelstepid,
        ts.travelsteptype

    from {{ ref('stg_tripdestination') }} td

    inner join {{ ref('stg_travelstep') }} ts
        on td.travelstepid = ts.travelstepid
),

tripdestination_with_granularity as (
    select
        t.tripid,
        t.travelstepid,

        case
        {% for type, granularity in trip_destination_granularities.items() %}
            when t.travelsteptype = '{{ type }}' then {{ granularity }}
        {% endfor %}
            else 0
        end destination_granularity,

        count(*) over (partition by tripid, destination_granularity) as num_with_same_granularity

    from tripdestination_ts t
),

tripdestination_most_granular_singular as (
    select
        td.tripid,
        td.travelstepid

    from tripdestination_with_granularity td

    where td.num_with_same_granularity = 1

    qualify row_number() over (
        partition by td.tripid
        order by td.destination_granularity desc
    ) = 1
),

final as (
    select
        t.tripid,
        coalesce(primarydestinationid, mgs.travelstepid) as definingdestinationid,

        current_timestamp as dbt_loadtime,
        '{{ invocation_id }}'::text as dbt_runid

    from trip t
    left join tripdestination_most_granular_singular mgs
        on t.tripid = mgs.tripid
)

select * from final