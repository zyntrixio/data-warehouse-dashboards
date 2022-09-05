WITH mock_brands AS (
    SELECT *
    FROM {{ref('trans__mock_brands')}}
)

,lc_join AS (
    SELECT *
    FROM {{ref('trans__lc_join')}}
)

,lc_removed AS (
    SELECT *
    FROM {{ref('src__fact_lc_removed')}}
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,dim_date AS (
    SELECT *
    FROM {{ref('src__dim_date')}}
)

,join_events as (
SELECT
        lcj.LOYALTY_CARD_ID
        ,lcj.EVENT_TYPE
        ,DATE(lcj.EVENT_DATE_TIME) AS DATE
        ,b.BRAND
        ,dlc.LOYALTY_PLAN_NAME
    FROM
        lc_join lcj
    LEFT JOIN mock_brands b
        ON lcj.USER_ID = b.USER_ID
    LEFT JOIN dim_lc dlc
        ON dlc.LOYALTY_CARD_ID = lcj.LOYALTY_CARD_ID

)

,aggregate_events AS (
    SELECT
        DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
        ,COUNT(CASE WHEN EVENT_TYPE = 'REQUEST' THEN 1 END) AS REQUESTED_JOINS
        ,COUNT(CASE WHEN EVENT_TYPE = 'FAILED' THEN 1 END) AS FAILED_JOINS
        ,COUNT(CASE WHEN EVENT_TYPE = 'SUCCESS' THEN 1 END) AS SUCCESSFUL_JOINS
    FROM
        join_events
    GROUP BY 
        DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
)

SELECT *
FROM aggregate_events
ORDER BY
    DATE
    ,BRAND
    ,LOYALTY_PLAN_NAME
