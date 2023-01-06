/*
Created at: 20/12/2022
Created by: Anand Bhakta
Description: Query to satisfy merchant reporting on adds
Modified at:
Modified by:
*/

WITH adds AS (
    SELECT
        *
    FROM {{ref('src__fact_lc_add')}}
)

,users AS (
    SELECT 
        *
    FROM {{ref('src__dim_user')}}
)

,extract_adds AS (
    SELECT
        DATE_TRUNC('month', a.EVENT_DATE_TIME) AS DATE
        ,a.LOYALTY_PLAN_NAME
        ,a.LOYALTY_CARD_ID
        ,a.AUTH_TYPE
        ,CONTAINS(u.EMAIL, 'e2e.bink.com') AS TESTER
    FROM
        adds a
    LEFT JOIN
        users u ON u.USER_ID = a.USER_ID
    WHERE
        a.AUTH_TYPE IN ('AUTH', 'ADD AUTH')
        AND 
        a.EVENT_TYPE = 'SUCCESS'
)

,metrics AS (
    SELECT
        DATE
        ,LOYALTY_PLAN_NAME
        ,NULL AS A001 --need pll
        ,NULL AS A002 --need pll
        ,COUNT(DISTINCT LOYALTY_CARD_ID) AS A003 --do we want to be counting a unique id?
    FROM 
        extract_adds
    WHERE
        TESTER = FALSE
    GROUP BY
        DATE
        ,LOYALTY_PLAN_NAME        
)

SELECT
    *
FROM
    metrics