{{ config(
    materialized='table',
    dist='reservationid',
    sort=['departureyear', 'departureyearmonth', 'reservationid'],
) }}

with passengerbio as (
    select * from {{ ref('int_reservationpassengerbio') }}
),

passengeragegroup as (

    select
        any_value(departureyear) as departureyear,
        any_value(departureyearmonth) as departureyearmonth,
        reservationid,

        count(passengerid) as numpassengers,
        sum(case when isadult then 1 else 0 end) as numadults,
        sum(case when ischild then 1 else 0 end) as numchildren,

        sum(case when exactageknown then 1 else 0 end) as numexactagepassengers,
        numpassengers = numexactagepassengers as allagesknown,

        sum(case when ageyears between 0 and {{ var('agecutoffs')['smallvsschooldchild'] - 1 }} then 1 else 0 end) as numsmallchildren,
        sum(case when ageyears between {{ var('agecutoffs')['smallvsschooldchild'] }} and {{ var('agecutoffs')['childvsadult'] - 1 }} then 1 else 0 end) as numschoolchildren,
        sum(case when ageyears between {{ var('agecutoffs')['childvsadult'] }} and {{ var('agecutoffs')['youngvsmiddleagedadult'] - 1 }} then 1 else 0 end) as numyoungadults,
        sum(case when ageyears between {{ var('agecutoffs')['youngvsmiddleagedadult'] }} and {{ var('agecutoffs')['middleagedvspensioners'] - 1 }} then 1 else 0 end) as nummiddleagedadults,
        sum(case when ageyears >= {{ var('agecutoffs')['middleagedvspensioners'] }} then 1 else 0 end) as numpensioners,

        {% for fn in ['min', 'avg', 'max'] %}
            {{ fn }}(case when isadult then ageyears else null end) as {{ fn }}adultage,
            {{ fn }}(case when ischild then ageyears else null end) as {{ fn }}childage,
        {% endfor %}

        case
            when numpassengers = 1 and numadults = 1 then 'AdultSolo'
            when numpassengers = 2 and numadults = 2 then 'AdultCouple'
            when numadults > 2 and numchildren = 0 then 'AdultGroup'
            when numadults >=1 and numsmallchildren >=1 then 'FamilyWithSmallChildren'
            when numadults >=1 and numschoolchildren >=1 then 'FamilyWithSchoolChildren'
            else 'Other'
        end as groupcategory,

        case
            when avgadultage < {{ var('agecutoffs')['youngvsmiddleagedadult'] }} then 'Young'
            when avgadultage >= {{ var('agecutoffs')['middleagedvspensioners'] }} then 'Pensioner'
            when avgadultage >= {{ var('agecutoffs')['youngvsmiddleagedadult'] }} then 'MiddleAged'
            else null
        end as adultagecategory

    from passengerbio
    group by reservationid

)

{# TODO add city info (another int. table) #}

select
    *,
    current_timestamp as dbt_loadtime,
    '{{ invocation_id }}'::text as dbt_runid

from passengeragegroup