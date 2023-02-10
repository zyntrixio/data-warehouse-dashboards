/*
Created by:         Anand Bhakta
Created date:       2023-02-05
Last modified by:   
Last modified date: 

Description:
    Rewrite of the LL table user_status_snapshot containing snapshot data of all user registrations.
Parameters:
    source_object      - trans__lbg_user, src__dim_date
*/

WITH user_statuses AS (
    SELECT *
    FROM {{ref('trans__lbg_user')}}
    WHERE EVENT IN ('CREATED', 'DELETED')
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
        ,EVENT AS FROM_EVENT
        ,DATE(EVENT_DATE_TIME) AS FROM_DATE
        ,DATE(LEAD(EVENT_DATE_TIME) OVER (PARTITION BY EXTERNAL_USER_REF ORDER BY EVENT_DATE_TIME)) AS TO_DATE
        ,LEAD(EVENT) OVER (PARTITION BY EXTERNAL_USER_REF ORDER BY EVENT_DATE_TIME) AS TO_EVENT
    FROM 
    user_statuses
)


,count_up AS (
  SELECT
    d.DATE
    ,u.BRAND
    ,COALESCE(SUM(CASE WHEN u.FROM_EVENT = 'CREATED' THEN 1 END),0) AS REGISTERED_USERS
    ,COALESCE(SUM(CASE WHEN u.FROM_EVENT = 'DELETED' THEN 1 END),0) AS DEREGISTERED_USERS
FROM to_from_dates u
LEFT JOIN dim_date d
    ON d.DATE >= u.FROM_DATE
    AND d.DATE < COALESCE(u.TO_DATE, '9999-12-31')
GROUP BY
    d.DATE
    ,u.BRAND
HAVING DATE IS NOT NULL
)

SELECT *
FROM count_up
