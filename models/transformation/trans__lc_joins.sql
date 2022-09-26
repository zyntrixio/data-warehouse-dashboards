WITH lc_join AS (
    SELECT *
    FROM {{ref('src__fact_lc_add')}}
    WHERE AUTH_TYPE IN ('JOIN', 'REGISTER')
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

SELECT *
FROM rank_events