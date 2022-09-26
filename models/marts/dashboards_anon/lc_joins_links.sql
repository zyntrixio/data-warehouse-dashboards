WITH lc_joins AS (
    SELECT *
    FROM {{ref('trans__lc_joins')}}
)

,lc_links AS (
    SELECT *
    FROM {{ref('trans__lc_links')}}
)

,mock_brands AS (
    SELECT *
    FROM {{ref('trans__mock_brands')}}
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,base_table AS (
    SELECT
        DATE
        ,LOYALTY_CARD_ID
        ,EVENT_TYPE
        ,USER_ID
        ,'JOIN' AS ADD_JOURNEY
    FROM lc_joins
    UNION ALL
    SELECT
        DATE
        ,LOYALTY_CARD_ID
        ,EVENT_TYPE
        ,USER_ID
        ,'LINK' AS ADD_JOURNEY
    FROM lc_links
)

,select_filter_columns AS (
    SELECT
        lc.DATE
        ,b.BRAND
        ,dlc.LOYALTY_PLAN_NAME
        ,EVENT_TYPE
        ,ADD_JOURNEY
    FROM
        base_table lc
    LEFT JOIN mock_brands b
        ON lc.USER_ID = b.USER_ID
    LEFT JOIN dim_lc dlc
        ON dlc.LOYALTY_CARD_ID = lc.LOYALTY_CARD_ID
    WHERE
        b.BRAND IS NOT NULL
        AND dlc.LOYALTY_PLAN_NAME IS NOT NULL
)

,aggregate_events AS (
    SELECT
        DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
        ,COUNT(CASE WHEN EVENT_TYPE = 'REQUEST' AND ADD_JOURNEY = 'JOIN' THEN 1 END) AS JOIN_REQUEST_PENDING
        ,COUNT(CASE WHEN EVENT_TYPE = 'FAILED' AND ADD_JOURNEY = 'JOIN' THEN 1 END) AS JOIN_FAILED_JOINS
        ,COUNT(CASE WHEN EVENT_TYPE = 'SUCCESS' AND ADD_JOURNEY = 'JOIN' THEN 1 END) AS JOIN_SUCCESSFUL_JOINS
        ,COUNT(CASE WHEN EVENT_TYPE = 'REQUEST' AND ADD_JOURNEY = 'LINK' THEN 1 END) AS LINK_REQUEST_PENDING
        ,COUNT(CASE WHEN EVENT_TYPE = 'FAILED' AND ADD_JOURNEY = 'LINK' THEN 1 END) AS LINK_FAILED_JOINS
        ,COUNT(CASE WHEN EVENT_TYPE = 'SUCCESS' AND ADD_JOURNEY = 'LINK' THEN 1 END) AS LINK_SUCCESSFUL_JOINS
    FROM
        select_filter_columns
    GROUP BY 
        DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
)

SELECT *
FROM aggregate_events