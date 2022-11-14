WITH lc_add AS (
    SELECT *
    FROM {{ref('src__fact_lc_add')}}
    WHERE AUTH_TYPE IN ('AUTH', 'ADD AUTH')
)

,lc_removed AS (
    SELECT *
    FROM {{ref('src__fact_lc_removed')}}
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

SELECT *
FROM union_new_old