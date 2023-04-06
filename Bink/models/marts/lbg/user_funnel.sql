/*
Created by:         Christopher Mitchell
Created date:       2023-04-06
Last modified by:   
Last modified date: 

Description:
   
Notes:
    
Parameters:
    src__fact_lc_add
*/

with lc as (
    select
        *
    from
        {{ref('src__fact_lc_add')}}
    where
        channel = 'LLOYDS'
    AND
        LOYALTY_PLAN_NAME NOT IN ('Bink Sweet Rewards','Loyalteas Plus')
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
)

,new_user_count_up as (
    select
        *,
        failures + resolved as attempt
    from
        user_count_up
),
new_count_up as (
    select
        attempt,
        auth_type,
        loyalty_plan_name,
        count(
            CASE
                WHEN resolved = 1 then resolved
            end
        ) successes,
        count(
            CASE
                WHEN resolved = 0 then resolved
            end
        ) end_state
    from
        new_user_count_up
    group by
        attempt,
        auth_type,
        loyalty_plan_name
),
new_window_count AS (
    SELECT
        attempt,
        auth_type,
        loyalty_plan_name,
        successes,
        end_state,
        sum(successes + end_state) OVER (
            PARTITION BY auth_type,
            loyalty_plan_name
            ORDER BY
                attempt DESC ROWS BETWEEN UNBOUNDED PRECEDING
                AND current ROW
        ) AS total
    FROM
        new_count_up
),
add_failures_precentages AS (
    SELECT
        attempt,
        auth_type,
        loyalty_plan_name,
        successes,
        end_state,
        total,
        total - successes AS failures,
        successes/total AS success_rate,
       end_state/total AS drop_off_rate
    FROM
        new_window_count
)
SELECT
    *
FROM
    add_failures_precentages
ORDER BY
    attempt