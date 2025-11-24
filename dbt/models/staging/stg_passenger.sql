{{
    config(
        materialized='incremental',
        unique_key='passengerid',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        dist='reservationid',
        sort=['reservationid']
    )
}}

with source as (
    select * from {{ source('erp_raw', 'ebdb_public_passenger') }}

    {% if is_incremental() %}
        where coalesce(modifytime, createtime) >= coalesce((select max(modifytime) from {{ this }}), '1989-06-28'::timestamp)
    {% endif %}
)

select

    id as passengerid,
    reservationid,
    personid,

    trunc(date_trunc('month', {{ datalake_hometime_to_timestamp('createtime') }})) as createyearmonth,
    coalesce(modifytime, createtime) as modifytime,

    origincityid,
    returncityid,
    owncar as hasowncar,
    floor(agemonths / 12)::integer as childageyears,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

from source
