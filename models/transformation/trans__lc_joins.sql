WITH mock_brands AS (
    SELECT *
    FROM {{ref('trans__mock_brands')}}
)

,lc_join AS (
    SELECT *
    FROM {{ref('src__fact_lc_add')}}
    WHERE AUTH_TYPE IN ('JOIN', 'REGISTER')
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,rank_events as (
    SELECT
        LOYALTY_CARD_ID
        ,EVENT_TYPE
        ,EVENT_DATE_TIME
        ,DATE(EVENT_DATE_TIME) AS DATE
        ,USER_ID
        ,ROW_NUMBER() OVER (PARTITION BY LOYALTY_CARD_ID, DATE, USER_ID ORDER BY EVENT_DATE_TIME DESC) AS DAY_ORDER
    FROM
        lc_join
    QUALIFY
        DAY_ORDER = 1 -- Selects just the last event of the day
)

,select_filter_columns AS (
    SELECT
        lcj.DATE
        ,b.BRAND
        ,dlc.LOYALTY_PLAN_NAME
        ,EVENT_TYPE AS JOIN_EVENT_TYPE
    FROM
        rank_events lcj
    LEFT JOIN mock_brands b
        ON lcj.USER_ID = b.USER_ID
    LEFT JOIN dim_lc dlc
        ON dlc.LOYALTY_CARD_ID = lcj.LOYALTY_CARD_ID
    WHERE
        b.BRAND IS NOT NULL
        AND dlc.LOYALTY_PLAN_NAME IS NOT NULL
)

-- ,aggregate_events AS (
--     SELECT
--         DATE
--         ,BRAND
--         ,LOYALTY_PLAN_NAME
--         ,COUNT(CASE WHEN EVENT_TYPE = 'REQUEST' THEN 1 END) AS REQUEST_PENDING
--         ,COUNT(CASE WHEN EVENT_TYPE = 'FAILED' THEN 1 END) AS FAILED_JOINS
--         ,COUNT(CASE WHEN EVENT_TYPE = 'SUCCESS' THEN 1 END) AS SUCCESSFUL_JOINS
--     FROM
--         select_filter_columns
--     GROUP BY 
--         DATE
--         ,BRAND
--         ,LOYALTY_PLAN_NAME
-- )

SELECT *
FROM select_filter_columns