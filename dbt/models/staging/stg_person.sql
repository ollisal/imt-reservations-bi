{{
  config(
    materialized = 'incremental',
    unique_key='personid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    dist='personid',
    sort=['personid']
  )
}}

{# IMT has anonymized some customers by setting their DoB to e.g. 1900/01/01, ignore these #}
{% set OLDEST_VALID_DOB = '1920-01-01' %}

with source as (
    select * from {{ source('erp_raw', 'ebdb_public_person') }}

    {% if is_incremental() %}
        where coalesce(modifytime, createtime) >= coalesce((select max(modifytime) from {{ this }}), '1989-06-28'::timestamp)
    {% endif %}
),

person_with_super_extraprops as (
    select
        *,
        json_parse(extraprops) as _super_extraprops
    from source
),

minimized_person as (
    select
        id as personid,
        coalesce(modifytime, createtime) as modifytime,

        {# Truncated for privacy reasons #}
        trunc(date_trunc('month', {{ datalake_hometime_to_timestamp('createtime') }})) as createyearmonth,
        trunc(date_trunc('month', {{ datalake_hometime_to_timestamp('customeraccountcreated') }})) as customeraccountcreateyearmonth,
        {# This one is a bit special, as the dates of birth happen to be stored with a time component, set to midnight - in DB time zone (timestamp) #}
        case
            when dateofbirth >= '{{ OLDEST_VALID_DOB }}'::date
            then trunc(date_trunc('month', dateofbirth))
        end as birthyearmonth,

        customeraccountcreateyearmonth is not null as hascustomeraccount,
        bonus > 0 as customeraccounthasbonus,

        left((_super_extraprops."postNumber")::text, 2) as postnumber2,
        {{ normalize_noun_case('_super_extraprops.city::text') }} as city,
        coalesce((_super_extraprops."clubOne")::text, '') != '' as hasclubone,
        _super_extraprops.gender::text as gender

    from person_with_super_extraprops
)

select

    personid,
    modifytime,

    createyearmonth,
    customeraccountcreateyearmonth,

    birthyearmonth,
    gender,

    postnumber2,
    city,

    hascustomeraccount,
    customeraccounthasbonus,
    hasclubone,

    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

from minimized_person