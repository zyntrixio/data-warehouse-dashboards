WITH mock_brands AS (
    SELECT *
    FROM {{ref('trans__mock_brands')}}
)

,lc_add AS (
    SELECT *
    FROM {{ref('src__fact_lc_add')}}
    WHERE AUTH_TYPE IN ('AUTH', 'ADD AUTH')
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
    WHERE
        DATE >= (SELECT MIN(EVENT_DATE_TIME) FROM lc_add)
        AND DATE <= CURRENT_DATE()
)

,refine_deletions AS (
    SELECT lcr.*
    FROM lc_removed lcr
    LEFT JOIN lc_add lca
        ON lcr.LOYALTY_CARD_ID = lca.LOYALTY_CARD_ID
        AND lcr.CHANNEL = lca.CHANNEL
        AND lcr.USER_ID = lca.USER_ID
    WHERE
        lca.LOYALTY_CARD_ID IS NOT NULL
        AND lca.CHANNEL IS NOT NULL
        AND lca.USER_ID IS NOT NULL
)

,union_new_old as (
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,CHANNEL
        ,USER_ID
    FROM lc_add
    UNION
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,'REMOVED' AS EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,CHANNEL
        ,USER_ID
    FROM refine_deletions  
)

,event_ordering AS (
    SELECT *
        ,COALESCE( LEAD(EVENT_DATE_TIME, 1) OVER (PARTITION BY CHANNEL, LOYALTY_CARD_ID, USER_ID ORDER BY EVENT_DATE_TIME), '9999-12-31'::DATE ) AS VALID_TO
        ,LEAD(EVENT_TYPE, 1) OVER (PARTITION BY CHANNEL, LOYALTY_CARD_ID, USER_ID ORDER BY EVENT_DATE_TIME) AS NEXT_EVENT
        ,LAG(EVENT_TYPE, 1 ) OVER (PARTITION BY CHANNEL, LOYALTY_CARD_ID, USER_ID ORDER BY EVENT_DATE_TIME) AS PREV_EVENT
    FROM union_new_old
    QUALIFY
        ((COALESCE(PREV_EVENT,'') != EVENT_TYPE )
        OR (NEXT_EVENT IS NULL AND EVENT_TYPE IS NULL))
)

,day_ends AS ( -- just get events that finish the day
    SELECT *
    FROM event_ordering
    WHERE
        EVENT_DATE_TIME::date != VALID_TO::date
)

,lc_start_end as (
    SELECT
        lc.EVENT_TYPE
        ,DATE(lc.EVENT_DATE_TIME) AS START_DATE
        ,DATEADD(day, -1, DATE(lc.VALID_TO)) AS END_DATE
        ,b.BRAND
        ,dlc.LOYALTY_PLAN_NAME
    FROM day_ends lc
    LEFT JOIN mock_brands b
        ON lc.USER_ID = b.USER_ID
    LEFT JOIN dim_lc dlc
        ON lc.LOYALTY_CARD_ID = dlc.LOYALTY_CARD_ID
)

,count_up AS (
    SELECT
        d.DATE
        ,lc.BRAND
        ,lc.LOYALTY_PLAN_NAME
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'SUCCESS' THEN 1 END),0) AS SUCCESS_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'FAILED' THEN 1 END),0) AS FAILED_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'REQUEST' THEN 1 END),0) AS REQUEST_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'REMOVED' THEN 1 END),0) AS REMOVED_STATE
    FROM lc_start_end lc
    LEFT JOIN DIM_DATE d
        ON d.DATE >= lc.START_DATE
        AND d.DATE < lc.END_DATE
    GROUP BY
        d.DATE
        ,lc.BRAND
        ,lc.LOYALTY_PLAN_NAME
    HAVING
        DATE IS NOT NULL
        AND LOYALTY_PLAN_NAME IS NOT NULL
        AND BRAND IS NOT NULL
)

SELECT *
FROM count_up
