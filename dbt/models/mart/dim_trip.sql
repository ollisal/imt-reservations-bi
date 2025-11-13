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

trip_destination_info as (

    select
        tripid,
        definingdestinationid
    from {{ ref('int_tripdestinationpriority') }}

),

travelstep_hierarchy as (

    select
        travelstepid,
        travelstepname,
        travelsteptype,
        cityid,
        cityname,
        countryid,
        countryname
    from {{ ref('int_travelstephierarchy') }}

),

final as (

    select
        trips.*,

        trip_type_info.definingtype,
        trip_type_info.iscruise,
        trip_type_info.ishoteltrip,
        trip_type_info.isspatrip,
        trip_type_info.istour,

        -- Generic destination info
        travelstep_hierarchy.travelstepid as destinationid,
        travelstep_hierarchy.travelstepname as destinationname,
        travelstep_hierarchy.travelsteptype as destinationtype,

        -- Land hierarchy
        case
            when travelstep_hierarchy.travelsteptype = 'Hotel' then travelstep_hierarchy.travelstepid
            else null
        end as destinationhotelid,
        case
            when travelstep_hierarchy.travelsteptype = 'Hotel' then travelstep_hierarchy.travelstepname
            else null
        end as destinationhotelname,
        travelstep_hierarchy.cityid as destinationcityid,
        travelstep_hierarchy.cityname as destinationcityname,
        travelstep_hierarchy.countryid as destinationcountryid,
        travelstep_hierarchy.countryname as destinationcountryname,

        -- Ship
        case
            when travelstep_hierarchy.travelsteptype = 'Ship' then travelstep_hierarchy.travelstepid
            else null
        end as destinationshipid,
        case
            when travelstep_hierarchy.travelsteptype = 'Ship' then travelstep_hierarchy.travelstepname
            else null
        end as destinationshipname,

        trip_phase_counts.tripnumphases,
        {% for type in var('tripphasetypes') %}
        trip_phase_counts.tripnum{{ type | lower }}phases,
        {% endfor %}

        current_timestamp as dbt_loadtime,
        '{{ invocation_id }}'::text as dbt_runid

    from trips

    inner join trip_type_info
        using (tripid)

    inner join trip_destination_info
        using (tripid)

    left join travelstep_hierarchy
        on trip_destination_info.definingdestinationid = travelstep_hierarchy.travelstepid

    inner join trip_phase_counts
        using (tripid)

    -- TODO create separate row for trip when own car is chosen?

)

select * from final