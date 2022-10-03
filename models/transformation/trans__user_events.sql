WITH users AS (
    SELECT *
    FROM {{ref('src__fact_user')}}
)

,brands AS (
    SELECT *
    FROM {{ref('trans__mock_brands')}}
)

,wallet_refreshes AS (
    SELECT
        USER_ID
        ,EVENT_DATE_TIME
        ,'REFRESH' AS EVENT
        ,NULL AS LOYALTY_PLAN_NAME
    FROM {{ref('trans__mock_wallet_refresh')}}
)

,transactions AS (
    SELECT *
    FROM {{ref('src__fact_transaction')}}
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,create_delete AS ( -- ensures only valid create and deletes are counted
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
    FROM create_delete
)

,deletes AS (
    SELECT
        USER_ID
        ,DELETE_DT AS EVENT_DATE_TIME
        ,'DELETE' AS EVENT
        ,NULL AS LOYALTY_PLAN_NAME
    FROM create_delete
    WHERE NEXT_EVENT = 'DELETED'
)

,transactions_join AS (
    SELECT
        t.USER_ID
        ,t.EVENT_DATE_TIME
        ,'TRANSACT' AS EVENT
        ,dlc.LOYALTY_PLAN_NAME
    FROM transactions t
    LEFT JOIN dim_lc dlc
        ON t.LOYALTY_CARD_ID = dlc.LOYALTY_CARD_ID
)

,all_together AS (
    SELECT * FROM creates
    UNION ALL
    SELECT * FROM deletes
    UNION ALL
    SELECT * FROM wallet_refreshes
    UNION ALL
    SELECT * FROM transactions_join
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

,add_history_columns AS (
    SELECT
        *
        ,MAX(CASE WHEN EVENT = 'REFRESH' THEN EVENT_DATE_TIME END) OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LAST_REFRESH_EVENT
        ,MAX(CASE WHEN EVENT = 'TRANSACT' THEN EVENT_DATE_TIME END) OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LAST_TRANSACT_EVENT
        ,MIN(CASE WHEN EVENT = 'TRANSACT' THEN EVENT_DATE_TIME END) OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS NEXT_TRANSACT_EVENT
    FROM
        add_brand
)

SELECT *
FROM add_history_columns