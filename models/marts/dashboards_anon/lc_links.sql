WITH mock_brands AS (
    SELECT *
    FROM {{ref('trans__mock_brands')}}
)

,lc_link AS (
    SELECT *
    FROM {{ref('trans__lc_link')}}
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,dim_date AS (
    SELECT *
    FROM {{ref('src__dim_date')}}
)

,link_events as (
SELECT
        lcj.LOYALTY_CARD_ID
        ,lcj.STATUS_GROUPING
        ,DATE(lcj.EVENT_DATE_TIME) AS DATE
        ,b.BRAND
        ,dlc.LOYALTY_PLAN_NAME
    FROM
        lc_link lcj
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
        ,COUNT(CASE WHEN STATUS_GROUPING = 'PENDING' THEN 1 END) AS PENDING_JOINS
        ,COUNT(CASE WHEN STATUS_GROUPING = 'FAILED' THEN 1 END) AS FAILED_JOINS
        ,COUNT(CASE WHEN STATUS_GROUPING = 'SUCCESS' THEN 1 END) AS SUCCESSFUL_JOINS
    FROM
        link_events
    GROUP BY 
        DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
)

,counting_columns AS (
    SELECT
        DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
        ,PENDING_JOINS
        ,FAILED_JOINS
        ,SUCCESSFUL_JOINS
        ,SUM(SUCCESSFUL_JOINS) OVER (PARTITION BY BRAND, LOYALTY_PLAN_NAME ORDER BY DATE ASC) AS TOTAL_SUCCESSFUL_JOINS
    FROM
        aggregate_events
)

SELECT *
FROM counting_columns
ORDER BY
    DATE
    ,BRAND
    ,LOYALTY_PLAN_NAME
