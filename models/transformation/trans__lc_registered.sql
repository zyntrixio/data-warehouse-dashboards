-- For the dashboard, 'registered' refers to all lc create events: add_auth, auth, join, and register.
-- This stages a union of these tables

WITH
lc_add_auth AS (
    SELECT *
    FROM {{ref('src__fact_lc_add_auth')}}
)

,lc_auth AS (
    SELECT *
    FROM {{ref('src__fact_lc_auth')}}
)

,lc_join AS (
    SELECT *
    FROM {{ref('src__fact_lc_join')}}
)

,lc_register AS (
    SELECT *
    FROM {{ref('src__fact_lc_register')}}
)

,all_lcs AS (
    SELECT
        EVENT_DATE_TIME
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN
        ,USER_ID
        ,'ADD_AUTH' AS ROUTE
    FROM
        lc_add_auth
    UNION ALL
    SELECT
        EVENT_DATE_TIME
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN
        ,USER_ID
        ,'AUTH' AS ROUTE
    FROM
        lc_auth
    UNION ALL
    SELECT
        EVENT_DATE_TIME
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN
        ,USER_ID
        ,'JOIN' AS ROUTE
    FROM
        lc_join
    UNION ALL
    SELECT
        EVENT_DATE_TIME
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN
        ,USER_ID
        ,'REGISTER' AS ROUTE
    FROM
        lc_register
)

SELECT *
FROM all_lcs
