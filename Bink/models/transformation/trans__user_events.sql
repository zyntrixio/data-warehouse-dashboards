WITH users AS (
    SELECT *
    FROM {{ref('src__fact_user')}}
)

,brands AS (
    SELECT *
    FROM {{ref('trans__brands')}}
)

,wallet_refreshes AS (
    SELECT
        USER_ID
        ,EVENT_DATE_TIME
        ,'REFRESH' AS EVENT
        ,NULL AS LOYALTY_PLAN_NAME
        ,NULL AS LOYALTY_PLAN_COMPANY
    FROM {{ref('src__fact_user')}}
    WHERE EVENT_TYPE = 'REFRESH'
)

,transactions AS (
    SELECT *
    FROM {{ref('src__fact_transaction')}}
)

,lc_add AS (
    SELECT *
    FROM {{ref('src__fact_lc_add')}}
    WHERE EVENT_TYPE = 'SUCCESS'
)

,lc_remove AS (
    SELECT *
    FROM {{ref('src__fact_lc_removed')}}
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,create_delete AS ( -- ensures only valid create and deletes are counted. Takes last create event.
    SELECT
        USER_ID
        ,EVENT_DATE_TIME
        ,LEAD(EVENT_TYPE) OVER(PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME) AS NEXT_EVENT
        ,COALESCE(LEAD(EVENT_DATE_TIME) OVER(PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME),CURRENT_TIMESTAMP()) AS DELETE_DT
    FROM users
    QUALIFY
        EVENT_TYPE = 'CREATED'
        AND (NEXT_EVENT IS NULL OR NEXT_EVENT = 'DELETED')
)

,creates AS (
    SELECT
        USER_ID
        ,EVENT_DATE_TIME
        ,'CREATE' AS EVENT
        ,NULL AS LOYALTY_PLAN_NAME
        ,NULL AS LOYALTY_PLAN_COMPANY
    FROM create_delete
)

,deletes AS (
    SELECT
        USER_ID
        ,DELETE_DT AS EVENT_DATE_TIME
        ,'DELETE' AS EVENT
        ,NULL AS LOYALTY_PLAN_NAME
        ,NULL AS LOYALTY_PLAN_COMPANY
    FROM create_delete
    WHERE NEXT_EVENT = 'DELETED'
)

,transactions_join AS (
    SELECT
        t.USER_ID
        ,t.EVENT_DATE_TIME
        ,'TRANSACT' AS EVENT
        ,dlc.LOYALTY_PLAN_NAME
        ,dlc.LOYALTY_PLAN_COMPANY
    FROM transactions t
    LEFT JOIN dim_lc dlc
        ON t.LOYALTY_CARD_ID = dlc.LOYALTY_CARD_ID
)

,lc_register_events AS (
    SELECT
        lc.USER_ID
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
        ,lc.EVENT_DATE_TIME
        ,'LC_REMOVE' AS EVENT
        ,dlc.LOYALTY_PLAN_NAME
        ,dlc.LOYALTY_PLAN_COMPANY
    FROM lc_remove lc
    LEFT JOIN dim_lc dlc
        ON lc.LOYALTY_CARD_ID = dlc.LOYALTY_CARD_ID
)

,all_together AS (--Above code is pointless this just unions all events for users all lc events and transactions.
    SELECT * FROM creates
    UNION ALL
    SELECT * FROM deletes
    UNION ALL
    SELECT * FROM wallet_refreshes
    UNION ALL
    SELECT * FROM transactions_join
    UNION ALL
    SELECT * FROM lc_register_events
    UNION ALL
    SELECT * FROM lc_remove_events
)

,add_brand AS (
    SELECT
        u.*
        ,b.BRAND
    FROM
        all_together u
    LEFT JOIN
        brands b ON u.USER_ID = b.USER_ID
)

,refresh_each_lp AS ( -- Add a refresh event for each registered LC. This is to help with activity statuses for LC only.
    SELECT
        u.USER_ID
        ,u.EVENT_DATE_TIME
        ,u.EVENT
        ,COALESCE(u.LOYALTY_PLAN_NAME, u2.LOYALTY_PLAN_NAME) AS LOYALTY_PLAN
        ,COALESCE(u.LOYALTY_PLAN_COMPANY, u2.LOYALTY_PLAN_COMPANY) AS LOYALTY_PLAN_COMPANY
        ,u.BRAND
    FROM
        add_brand u
    LEFT JOIN add_brand u2
        ON u.USER_ID = u2.USER_ID
        AND u.BRAND = u2.BRAND
        AND u.EVENT = 'REFRESH'  
        AND u2.EVENT = 'LC_REGISTER'
        AND u2.EVENT_DATE_TIME < u.EVENT_DATE_TIME
)

SELECT *
FROM refresh_each_lp


