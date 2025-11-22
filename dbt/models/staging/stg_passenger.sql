{{
    config(
        materialized='table',
        dist='reservationid',
        sort=['reservationid']
    )
}}

with source as (select * from {{ source('erp_raw', 'ebdb_public_passenger') }})

select

    id as passengerid,
    reservationid,
    personid,

    trunc(date_trunc('month', {{ datalake_hometime_to_timestamp('createtime') }})) as createyearmonth,

    origincityid,
    returncityid,
    owncar as hasowncar,
    agemonths as childagemonths,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

from source
