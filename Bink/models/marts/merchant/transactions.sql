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
        ,AVG(CASE FEED_TYPE WHEN 'SETTLED' THEN SPEND_AMOUNT END) AS T002b
        ,SUM(SPEND_AMOUNT) AS T003b
        ,SUM(CASE FEED_TYPE WHEN 'SETTLED' THEN SPEND_AMOUNT END) AS T004b
        ,SUM(CASE FEED_TYPE WHEN 'REFUND' THEN SPEND_AMOUNT END) AS T005b
        ,COUNT(DISTINCT TRANSACTION_ID)/COUNT(DISTINCT IDENTIFIER) AS T006b
        ,SUM(SPEND_AMOUNT)/COUNT(DISTINCT IDENTIFIER) AS T007b
        ,NULL AS T008b --missing txn response
        ,NULL AS T009b --missing txn response
        ,NULL AS T010b --missing txn response
        ,NULL AS T011b --missing txn response
        ,COUNT(DISTINCT TRANSACTION_ID) T012b
        ,COUNT(DISTINCT CASE FEED_TYPE WHEN 'SETTLED' THEN TRANSACTION_ID END) AS T013b
        ,NULL AS T014b --cumulative
        ,NULL AS T015b --cumulative
        ,NULL AS T016b --missing txn response

    FROM
        add_testers
    WHERE
        TESTER = FALSE
    GROUP BY
        DATE
        ,LOYALTY_PLAN_NAME
)

,expand_metrics AS (
    SELECT
        DATE
        ,LOYALTY_PLAN_NAME
        ,T001b
        ,T002b
        ,T003b
        ,T004b
        ,T005b
        ,T006b
        ,T007b
        ,T008b
        ,T009b
        ,T010b
        ,T011b
        ,T012b
        ,T013b
        ,SUM(T012b) OVER (PARTITION BY LOYALTY_PLAN_NAME ORDER BY DATE ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS T014b
        ,SUM(T013b) OVER (PARTITION BY LOYALTY_PLAN_NAME ORDER BY DATE ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS T015b
        ,T016b
    FROM
        metrics
)

SELECT
    *
FROM
    expand_metrics
ORDER BY
    DATE
    ,LOYALTY_PLAN_NAME
