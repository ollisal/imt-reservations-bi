{{ config(materialized="table", diststyle="all", sortkey=["tripid"]) }}

with source as (select * from {{ source("erp_raw", "ebdb_public_trip") }})

select

    id as tripid,
    name,
    primarydestinationid,
    primarytype,
    tripduration,
    allowowncar,
    requireowncar,
    isarchived,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}' as dbt_runid

from source
