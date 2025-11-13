{{config(
    materialized='view',
)}}

{# If a trip doesn't have primarytype, we use the "most defining" type or concatenation if equally weighty #}
{% set trip_type_priorities={'Tour': 3, 'SpaTrip': 2, 'HotelTrip': 1, 'Cruise': 1} %}

with trip as (

    select
        tripid,
        primarytype
    from {{ ref('stg_trip') }}

),

triptype as (

    select
        tripid,
        type
    from {{ ref('stg_triptype') }}
),

triptypes_prioritized as (
    select
        t.tripid,
        t.type,

        case
        {% for type, priority in trip_type_priorities.items() %}
            when t.type = '{{ type }}' then {{ priority }}
        {% endfor %}
            else 0
        end type_priority
    from triptype t
),

triptypes_concatenated as (
    select
        t.tripid,
        listagg(t.type, '-') within group (order by t.type asc {# Cruise-HotelTrip #}) as concatenated_types
    from triptypes_prioritized t
    where type_priority = (
        select max(type_priority)
        from triptypes_prioritized t2
        where t2.tripid = t.tripid
    )
    group by t.tripid
),

trip_with_definingtype as (
    select
        t.tripid,
        coalesce(
            t.primarytype,
            tc.concatenated_types
        ) as definingtype

    from trip t
    left join triptypes_concatenated tc
        using (tripid)
),

trip_with_type_flags as (
    select
        t.tripid
        {% for type in trip_type_priorities.keys()|sort %}
            , coalesce(
                bool_or(tp.type = '{{ type }}'),
                false
            ) as is{{ type|lower }}
        {% endfor %}

    from trip t
    left join triptype tp
        using (tripid)

    group by t.tripid
),

final as (
    select
        *,

        current_timestamp as dbt_loadtime,
        '{{ invocation_id }}'::text as dbt_runid

    from trip_with_definingtype
    inner join trip_with_type_flags
    using (tripid)
)

select * from final