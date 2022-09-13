WITH mock_brands AS (
    SELECT *
    FROM {{ref('trans__mock_brands')}}
)

,lc_add AS (
    SELECT *
    FROM {{ref('src__fact_lc_add')}}
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,dim_date AS (
    SELECT *
    FROM {{ref('src__dim_date')}}
)

,lc_start as (
    SELECT
        lca.LOYALTY_CARD_ID
        ,lca.AUTH_TYPE
        ,DATE(lca.EVENT_DATE_TIME) AS DATE
        ,b.BRAND
        ,dlc.LOYALTY_PLAN_NAME
    FROM lc_add lca
    LEFT JOIN mock_brands b
        ON lca.USER_ID = b.USER_ID
    LEFT JOIN dim_lc dlc
        ON dlc.LOYALTY_CARD_ID = lca.LOYALTY_CARD_ID
    WHERE
        EVENT_TYPE = 'SUCCESS'
)

,aggregate_up AS (
    SELECT
        DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
        ,COALESCE(SUM(CASE WHEN AUTH_TYPE IN ('AUTH', 'ADD AUTH') THEN 1 END),0) AS NEW_LOYALTY_LINKS
        ,COALESCE(SUM(CASE WHEN AUTH_TYPE IN ('JOIN', 'REGISTER') THEN 1 END),0) AS NEW_LOYALTY_JOINS
    FROM lc_start
    GROUP BY
        DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
)
      

SELECT *
FROM aggregate_up