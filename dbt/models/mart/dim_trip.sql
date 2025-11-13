{{ config(
    materialized='table',
    dist='all',
    sort=['tripid'],
) }}

with trips as (

    select
        tripid,
        name as tripname,
        isarchived as tripisarchived,
        allowowncar,
        requireowncar
    from {{ ref('stg_trip') }}

),

trip_type_info as (

    select
        tripid,
        definingtype,
        iscruise,
        ishoteltrip,
        isspatrip,
        istour
    from {{ ref('int_triptypepriority') }}

),

trip_phase_counts as (

    select
        tripid,
        {% for type in var('tripphasetypes') %}
        tripnum{{ type | lower }}phases,
        {% endfor %}
        tripnumphases
    from {{ ref('int_tripphasecounts') }}

),

final as (

    select
        trips.*,

        trip_type_info.definingtype,
        trip_type_info.iscruise,
        trip_type_info.ishoteltrip,
        trip_type_info.isspatrip,
        trip_type_info.istour,

        trip_phase_counts.tripnumphases,
        {% for type in var('tripphasetypes') %}
        trip_phase_counts.tripnum{{ type | lower }}phases,
        {% endfor %}

        current_timestamp as dbt_loadtime,
        '{{ invocation_id }}'::text as dbt_runid

    from trips

    inner join trip_type_info
        using (tripid)

    inner join trip_phase_counts
        using (tripid)

    -- TODO add destination info
    -- TODO create separate row for trip when own car is chosen?

)

select * from final

