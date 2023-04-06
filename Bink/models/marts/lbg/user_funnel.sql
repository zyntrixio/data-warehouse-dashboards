/*
Created by:         Christopher Mitchell
Created date:       2023-04-06
Last modified by:   
Last modified date: 

Description:
   
Notes:
    
Parameters:

*/

with lc as (
    select
        *
    from
        {{ref('src__fact_lc_add')}}
),
lc_group as (
    SELECT
        EXTERNAL_USER_REF,
        EVENT_DATE_TIME,
        AUTH_TYPE,
        EVENT_TYPE,
        LOYALTY_CARD_ID,
        LOYALTY_PLAN_NAME,
        SUM(
            CASE
                WHEN EVENT_TYPE = 'SUCCESS' THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY EXTERNAL_USER_REF,
            LOYALTY_PLAN,
            AUTH_TYPE
            ORDER BY
                EVENT_DATE_TIME ASC
        ) as lc_group
    from
        lc
),
lc_group2 as (
    SELECT
        EXTERNAL_USER_REF,
        EVENT_DATE_TIME,
        AUTH_TYPE,
        EVENT_TYPE,
        LOYALTY_CARD_ID,
        LOYALTY_PLAN_NAME,
        CASE
            WHEN EVENT_TYPE = 'SUCCESS' THEN lc_group -1
            ELSE lc_group
        END as lc_group
    from
        lc_group
),
user_count_up as (
    select
        EXTERNAL_USER_REF,
        LOYALTY_PLAN_NAME,
        auth_type,
        lc_group,
        sum(
            CASE
                WHEN EVENT_TYPE = 'FAILED' THEN 1
                ELSE 0
            END
        ) as failures,
        sum(
            CASE
                WHEN EVENT_TYPE = 'SUCCESS' THEN 1
                ELSE 0
            END
        ) as resolved
    from
        lc_group2
    group by
        external_user_ref,
        lc_group,
        auth_type,
        LOYALTY_PLAN_NAME
),
count_up as (
    select
        failures,
        auth_type,
        loyalty_plan_name,
        count(
            CASE
                WHEN resolved > 0 THEN lc_group
            END
        ) successes,
        count(
            CASE
                WHEN resolved = 0 tHEN lc_group
            END
        ) end_states,
        count(
            CASE
                WHEN resolved > 0 THEN lc_group
            END
        ) / count(lc_group) as success_rate
    from
        user_count_up
    group by
        failures,
        auth_type,
        loyalty_plan_name
),
count_up as (
    select
        failures,
        auth_type,
        loyalty_plan_name,
        count(
            CASE
                WHEN resolved > 0 THEN lc_group
            END
        ) successes,
        count(
            CASE
                WHEN resolved = 0 THEN lc_group
            END
        ) end_states,
        count(
            CASE
                WHEN resolved > 0 THEN lc_group
            END
        ) / count(lc_group) as success_rate
    from
        user_count_up
    group by
        failures,
        auth_type,
        loyalty_plan_name
),
window_count as (
    SELECt
        failures,
        auth_type,
        loyalty_plan_name,
        successes,
        end_states,
        success_rate,
        sum(successes + end_states) over (
            partition by auth_type,
            loyalty_plan_name
            order by
                failures desc rows between unbounded preceding
                and current row
        ) total,
        sum(successes) over (
            partition by auth_type,
            loyalty_plan_name
            order by
                failures desc rows between unbounded preceding
                and current row
        ) cumulative_success,
        sum(end_states) over (
            partition by auth_type,
            loyalty_plan_name
            order by
                failures desc rows between unbounded preceding
                and current row
        ) cumulative_fail
    from
        count_up
),
window_count_2 as (
    select
        failures,
        auth_type,
        loyalty_plan_name,
        total,
        successes,
        end_states,
        success_rate,
        cumulative_success,
        cumulative_success / (cumulative_success + cumulative_fail) as cumulative_success_rate
    from
        window_count
)
select
    *
from
    window_count_2
order by
    loyalty_plan_name,
    auth_type,
    failures;