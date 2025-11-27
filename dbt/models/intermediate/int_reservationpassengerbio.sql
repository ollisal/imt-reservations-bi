{{config(
    materialized='view',
)}}

with reservation as (

    select
        reservationid,

        departuredate,

        firstundecidedreservationview,
        firstundecidedstepindex

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

        trunc(date_trunc('month', r.departuredate)) as departureyearmonth,
        firstundecidedreservationview != 'ProductSelection' or firstundecidedstepindex > 0 as respastdeparturestep

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
        case
            when ageyears >= 18 then true
            when ageyears is null and respastdeparturestep then true
            else false
        end as isdefiniteadult,
        coalesce(ageyears < 18, false) as isdefinitechild,

        current_timestamp as dbt_loadtime,
        '{{ invocation_id }}'::text as dbt_runid

    from passenger_with_res_info p
    left join person pe using (personid)
)

select * from final