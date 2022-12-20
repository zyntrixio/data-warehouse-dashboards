/*
Created at: 02/12/2022
Created by: Anand Bhakta
Description: Queries to satisfy merchant reporting
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
        ,t.TRANSACTION_ID
        ,t.FEED_TYPE --Only in DEV currently
        ,t.SPEND_AMOUNT
    FROM
        export_transactions t
    LEFT JOIN
        users u ON u.USER_ID = t.USER_ID
)

,metrics AS (
    SELECT
        DATE
        ,LOYALTY_PLAN_NAME
        ,AVG(SPEND_AMOUNT) AS T001b
        ,AVG(CASE FEED_TYPE 
                    WHEN 'SETTLED' 
                    THEN SPEND_AMOUNT
                    END) AS T002b
        ,SUM(SPEND_AMOUNT) AS T003b
        ,SUM(CASE FEED_TYPE 
                    WHEN 'SETTLED' 
                    THEN SPEND_AMOUNT
                    END) AS T004b
        ,SUM(CASE FEED_TYPE 
                    WHEN 'REFUND' 
                    THEN SPEND_AMOUNT
                    END) AS T005b
        ,NULL AS T006b -- missing spotted transactions
        ,NULL AS T007b -- missing spotted transactions
        ,COUNT(DISTINCT TRANSACTION_ID)/COUNT(DISTINCT IDENTIFIER) AS T008b
        ,SUM(SPEND_AMOUNT)/COUNT(DISTINCT IDENTIFIER) AS T009b
        ,COUNT(DISTINCT TRANSACTION_ID) AS T010b
        ,NULL AS W001b
        ,NULL AS W002b
        ,NULL AS W003b

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




