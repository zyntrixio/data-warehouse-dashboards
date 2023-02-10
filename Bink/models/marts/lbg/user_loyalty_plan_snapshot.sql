/*
Created by:         Anand Bhakta
Created date:       2023-02-05
Last modified by:   
Last modified date: 

Description:
    Rewrite of the LL table user_loyalty_plan_status_snapshot containing snapshot data of all user registrations with loyalty cards split by merchant.

Notes:
    This code can be made more efficient if the start is pushed to the trans__lbg_user code and that can be the source for the majority of the dashboards including user_loyalty_plan_snapshot and user_with_loyalty_cards

Parameters:
    source_object      - trans__lbg_user, src__dim_date
*/

WITH user_statuses AS (
    SELECT *
    FROM {{ref('trans__lbg_user')}}
    WHERE EVENT IN ('LC_REGISTER', 'LC_REMOVE')
)

,dim_date AS (
    SELECT *
    FROM {{ref('src__dim_date')}}
    WHERE
        DATE >= (SELECT MIN(DATE(EVENT_DATE_TIME)) FROM user_statuses)
        AND DATE <= CURRENT_DATE()
)

,to_from_dates AS (
    SELECT
        USER_ID
        ,BRAND
        ,EXTERNAL_USER_REF
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
        ,EVENT AS FROM_EVENT
        ,DATE(EVENT_DATE_TIME) AS FROM_DATE
        ,DATE(LEAD(EVENT_DATE_TIME) OVER (PARTITION BY EXTERNAL_USER_REF, LOYALTY_PLAN_NAME ORDER BY EVENT_DATE_TIME)) AS TO_DATE
        ,LEAD(EVENT) OVER (PARTITION BY EXTERNAL_USER_REF, LOYALTY_PLAN_NAME ORDER BY EVENT_DATE_TIME) AS TO_EVENT
    FROM 
    user_statuses
)


,count_up_snap AS (
  SELECT
    d.DATE
    ,u.BRAND
    ,u.LOYALTY_PLAN_NAME
    ,u.LOYALTY_PLAN_COMPANY
    ,COALESCE(SUM(CASE WHEN u.FROM_EVENT = 'LC_REGISTER' THEN 1 END),0) AS REGISTERED_USERS
    ,COALESCE(SUM(CASE WHEN u.FROM_EVENT = 'LC_REMOVE' THEN 1 END),0) AS DEREGISTERED_USERS
FROM to_from_dates u
LEFT JOIN dim_date d
    ON d.DATE >= u.FROM_DATE
    AND d.DATE < COALESCE(u.TO_DATE, '9999-12-31')
GROUP BY
    d.DATE
    ,u.BRAND
    ,u.LOYALTY_PLAN_NAME
    ,u.LOYALTY_PLAN_COMPANY
HAVING DATE IS NOT NULL
)

,count_up_abs AS (
  SELECT
    d.DATE
    ,u.BRAND
    ,u.LOYALTY_PLAN_NAME
    ,u.LOYALTY_PLAN_COMPANY
    ,COALESCE(SUM(CASE WHEN u.FROM_EVENT = 'LC_REGISTER' THEN 1 END),0) AS DAILY_REGISTRATIONS
    ,COALESCE(SUM(CASE WHEN u.FROM_EVENT = 'LC_REMOVE' THEN 1 END),0) AS DAILY_DEREGISTRATIONS
FROM to_from_dates u
LEFT JOIN dim_date d
    ON d.DATE = u.FROM_DATE
GROUP BY
    d.DATE
    ,u.BRAND
    ,u.LOYALTY_PLAN_NAME
    ,u.LOYALTY_PLAN_COMPANY
HAVING DATE IS NOT NULL
)

,all_together AS (
    SELECT
        COALESCE(a.DATE,s.DATE) DATE
        ,COALESCE(a.BRAND,s.BRAND) BRAND
        ,COALESCE(a.LOYALTY_PLAN_NAME,s.LOYALTY_PLAN_NAME) LOYALTY_PLAN_NAME
        ,COALESCE(a.LOYALTY_PLAN_COMPANY,s.LOYALTY_PLAN_COMPANY) LOYALTY_PLAN_COMPANY
        ,COALESCE(a.DAILY_REGISTRATIONS,0) DAILY_REGISTRATIONS
        ,COALESCE(a.DAILY_DEREGISTRATIONS,0) DAILY_DEREGISTRATIONS
        ,COALESCE(s.REGISTERED_USERS,0) REGISTERED_USERS
        ,COALESCE(s.DEREGISTERED_USERS,0) DEREGISTERED_USERS
    FROM count_up_abs a
    FULL OUTER JOIN count_up_snap s 
        ON a.DATE=s.DATE and a.BRAND = s.BRAND and a.LOYALTY_PLAN_NAME = s.LOYALTY_PLAN_NAME
)

SELECT *
FROM all_together
where LOYALTY_PLAN_NAME is not null --this can be removed when datafix implemented
