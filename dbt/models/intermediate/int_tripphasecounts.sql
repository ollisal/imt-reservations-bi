{{config(
    materialized='view',
)}}

with trips as (

    select
        tripid
    from {{ ref('stg_trip') }}

),

tripphases as (

    select
        tripid,
        tripphasetype
    from {{ ref('stg_tripphase') }}

),

phase_counts as (

    select
        t.tripid,

        {% for type in var('tripphasetypes') %}
        sum(case when tp.tripphasetype = '{{ type }}' then 1 else 0 end) as tripnum{{ type | lower }}phases,
        {% endfor %}
        count(tp.tripphasetype) as tripnumphases

    from trips t
    left join tripphases tp
        on t.tripid = tp.tripid

    group by t.tripid

),

final as (

    select
        *,

        current_timestamp as dbt_loadtime,
        '{{ invocation_id }}'::text as dbt_runid

    from phase_counts

)

select * from final
