/*
Created at: 20/12/2022
Created by: Anand Bhakta
Description: Query to satisfy merchant reporting on adds
Modified at:
Modified by:
*/

WITH joins AS (
    SELECT
        *
    FROM {{ref('src__fact_lc_add')}}
)

,users AS (
    SELECT 
        *
    FROM {{ref('src__dim_user')}}
)

,extract_joins AS (
    SELECT
        DATE_TRUNC('month', j.EVENT_DATE_TIME) AS DATE
        ,j.LOYALTY_PLAN
        ,j.LOYALTY_CARD_ID
        ,j.AUTH_TYPE
        ,CONTAINS(u.EMAIL, 'e2e.bink.com') AS TESTER
    FROM
        joins j
    LEFT JOIN
        users u ON u.USER_ID = j.USER_ID
    WHERE
        j.AUTH_TYPE IN ('AUTH', 'ADD AUTH')
        AND 
        j.EVENT_TYPE = 'SUCCESS'
)

,metrics AS (
    SELECT
        DATE
        ,LOYALTY_PLAN
        ,COUNT(DISTINCT LOYALTY_CARD_ID) AS A001 --do we want to be counting a unique id?
        ,NULL AS A002 --need pll
        ,NULL AS A003
    FROM 
        extract_joins
    WHERE
        TESTER = FALSE
    GROUP BY
        DATE
        ,LOYALTY_PLAN        
)

SELECT
    *
FROM
    metrics