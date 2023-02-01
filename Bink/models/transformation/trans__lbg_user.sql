WITH users AS (
    SELECT *
    FROM {{ref('src__fact_user')}}
    WHERE EVENT_TYPE != 'REFRESH'
    AND CHANNEL = 'LLOYDS'
)

,lc_add AS (
    SELECT *
    FROM {{ref('src__fact_lc_add')}}
    WHERE EVENT_TYPE = 'SUCCESS'
    AND CHANNEL = 'LLOYDS'
)

,lc_remove AS (
    SELECT *
    FROM {{ref('src__fact_lc_removed')}}
    WHERE CHANNEL = 'LLOYDS'
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,user_events AS (
    SELECT
        USER_ID
        ,EXTERNAL_USER_REF
        ,BRAND
        ,EVENT_DATE_TIME
        ,EVENT_TYPE AS EVENT
        ,NULL AS LOYALTY_PLAN_NAME
        ,NULL AS LOYALTY_PLAN_COMPANY
    FROM users
)

,lc_register_events AS (
    SELECT
        lc.USER_ID
        ,lc.EXTERNAL_USER_REF
        ,lc.BRAND
        ,lc.EVENT_DATE_TIME
        ,'LC_REGISTER' AS EVENT
        ,dlc.LOYALTY_PLAN_NAME
        ,dlc.LOYALTY_PLAN_COMPANY
    FROM lc_add lc
    LEFT JOIN dim_lc dlc
        ON lc.LOYALTY_CARD_ID = dlc.LOYALTY_CARD_ID
)

,lc_remove_events AS (
    SELECT
        lc.USER_ID
        ,lc.EXTERNAL_USER_REF
        ,lc.BRAND
        ,lc.EVENT_DATE_TIME
        ,'LC_REMOVE' AS EVENT
        ,dlc.LOYALTY_PLAN_NAME
        ,dlc.LOYALTY_PLAN_COMPANY
    FROM lc_remove lc
    LEFT JOIN dim_lc dlc
        ON lc.LOYALTY_CARD_ID = dlc.LOYALTY_CARD_ID
)

,all_together AS (--Above code is pointless this just unions all events for users all lc events and transactions.
    SELECT * FROM user_events
    UNION ALL
    SELECT * FROM lc_register_events
    UNION ALL
    SELECT * FROM lc_remove_events
)

--missing the add refresh to each LC from LL code not sure if this is needed
select * from all_together