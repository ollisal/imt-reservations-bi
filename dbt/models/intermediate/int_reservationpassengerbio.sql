{{config(
    materialized='view',
)}}

with reservation as (

    select
        reservationid,

        departuredate

    from {{ ref('stg_reservation') }}

),

passenger as (
    select
        passengerid,
        reservationid,
        personid,

        childageyears

    from {{ ref('stg_passenger') }}
),

person as (
    select
        personid,

        birthyearmonth,
        gender

    from {{ ref('stg_person') }}
),

passenger_with_res_info as (
    select
        p.passengerid,
        p.reservationid,
        p.personid,
        childageyears,

        trunc(date_trunc('month', r.departuredate)) as departureyearmonth

    from passenger p
    inner join reservation r using (reservationid)

    where r.departuredate is not null
),

final as (
    select

        p.passengerid,
        p.reservationid,
        p.personid,

        extract(year from departureyearmonth) as departureyear,
        departureyearmonth,
        pe.birthyearmonth,

        pe.gender,

        case
            when p.departureyearmonth >= pe.birthyearmonth then (months_between(p.departureyearmonth, pe.birthyearmonth) / 12)::integer
            when p.childageyears is not null then p.childageyears
        end as ageyears,

        case
            when p.departureyearmonth >= pe.birthyearmonth then 'persondetails'::text
            when p.childageyears is not null then 'childpassenger'::text
        end as agesource,

        ageyears is not null as exactageknown,
        coalesce(ageyears >= 18, true) as isadult,
        coalesce(ageyears < 18, false) as ischild,

        current_timestamp as dbt_loadtime,
        '{{ invocation_id }}'::text as dbt_runid

    from passenger_with_res_info p
    left join person pe using (personid)
)

select * from final