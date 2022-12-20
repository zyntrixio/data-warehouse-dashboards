/*
Created at: 02/12/2022
Created by: Anand Bhakta
Description: An attempt to write queries to satisfy active users section of merchant reporting
Modified at:
Modified by:
*/

--METRIC: AU001b
--STATUS: DRAFT
--DESCRIPTION: Active Users (Matched)
--NOTES: Remove testers

--METRIC: AU002b
--STATUS: DRAFT
--DESCRIPTION: Active Users (Settlement only)
--NOTES: Remove testers

--METRIC: AU003b
--STATUS: DRAFT
--DESCRIPTION: Active Users (Spotted)
--NOTES: Remove testers


WITH export_transactions AS (
    SELECT
        *
    FROM {{ref('src__fact_transaction')}}
)

,users AS (
    SELECT 
        *
    FROM {{ref('src__dim_user')}}
)

,add_testers AS (
    SELECT 
        DATE(DATE_TRUNC('month',t.event_date_time)) AS DATE
        ,t.LOYALTY_PLAN_NAME
        ,CONTAINS(u.EMAIL, 'e2e.bink.com') AS TESTER
        ,t.LOYALTY_ID AS IDENTIFIER
        ,t.FEED_TYPE --Only in DEV currently
    FROM
        export_transactions t
    LEFT JOIN
        users u ON u.USER_ID = t.USER_ID
)

,metrics AS (
    SELECT
        DATE
        ,LOYALTY_PLAN_NAME
        ,COUNT(DISTINCT IDENTIFIER) AS AU001b
        ,COUNT(DISTINCT
            CASE FEED_TYPE 
                WHEN 'SETTLED' 
                THEN IDENTIFIER
                END) AS AU002b
        ,COUNT(DISTINCT IDENTIFIER) AS AU003b
    FROM
        add_testers
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
ORDER BY
    DATE
    ,LOYALTY_PLAN_NAME




