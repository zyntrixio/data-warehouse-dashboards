WITH mock_brands AS (
    SELECT *
    FROM {{ref('trans__brands')}}
)

,lc_joins AS (
    SELECT *
    FROM {{ref('trans__lc_joins_create_delete')}}
)

,lc_links AS (
    SELECT *
    FROM {{ref('trans__lc_links_create_delete')}}
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,dim_date AS (
    SELECT *
    FROM {{ref('src__dim_date')}}
    WHERE
        DATE >= (SELECT MIN(d) FROM (SELECT EVENT_DATE_TIME::DATE d FROM lc_joins UNION SELECT EVENT_DATE_TIME::DATE d FROM lc_links))
        AND DATE <= CURRENT_DATE()
)

,union_joins_links AS (
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,CHANNEL
        ,USER_ID
        ,'JOIN' AS ADD_JOURNEY
    FROM
        lc_joins
    UNION ALL
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,CHANNEL
        ,USER_ID
        ,'LINK' AS ADD_JOURNEY
    FROM
        lc_links
)

,add_loyalty_plan_and_brand AS (
    SELECT
        lc.*
        ,b.BRAND
        ,dlc.LOYALTY_PLAN_NAME
        ,dlc.LOYALTY_PLAN_COMPANY
    FROM union_joins_links lc
    LEFT JOIN mock_brands b
        ON lc.USER_ID = b.USER_ID
    LEFT JOIN dim_lc dlc
        ON lc.LOYALTY_CARD_ID = dlc.LOYALTY_CARD_ID
)

,event_ordering AS (
    SELECT
        *
        ,COALESCE( LEAD(EVENT_DATE_TIME, 1) OVER (PARTITION BY ADD_JOURNEY, LOYALTY_PLAN_NAME, USER_ID ORDER BY EVENT_DATE_TIME), '9999-12-31'::DATE ) AS VALID_TO
        ,LEAD(EVENT_TYPE, 1) OVER (PARTITION BY ADD_JOURNEY, LOYALTY_PLAN_NAME, USER_ID ORDER BY EVENT_DATE_TIME) AS NEXT_EVENT
        ,LAG(EVENT_TYPE, 1 ) OVER (PARTITION BY ADD_JOURNEY, LOYALTY_PLAN_NAME, USER_ID ORDER BY EVENT_DATE_TIME) AS PREV_EVENT
    FROM add_loyalty_plan_and_brand
    QUALIFY
        ((COALESCE(PREV_EVENT,'') != EVENT_TYPE )
        OR (NEXT_EVENT IS NULL AND EVENT_TYPE IS NULL))
)

,refine_error_states AS ( -- Remove events going from Failed to Removed. These are modelled as error states
    SELECT *
    FROM event_ordering
    WHERE EVENT_TYPE != 'REMOVED' OR PREV_EVENT != 'FAILED'
)

,day_ends AS ( -- just get events that finish the day
    SELECT
        *
        ,MAX(EVENT_DATE_TIME) OVER (PARTITION BY ADD_JOURNEY, LOYALTY_PLAN_NAME, USER_ID) AS LAST_EVENT_DAILY
    FROM refine_error_states
    QUALIFY
        EVENT_DATE_TIME = LAST_EVENT_DAILY
)

,lc_start_end as (
    SELECT
        EVENT_TYPE
        ,ADD_JOURNEY
        ,DATE(EVENT_DATE_TIME) AS START_DATE
        ,DATEADD(day, -1, DATE(VALID_TO)) AS END_DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_COMPANY
    FROM day_ends
)

,count_up AS (
    SELECT
        d.DATE
        ,lc.BRAND
        ,lc.LOYALTY_PLAN_NAME
        ,lc.LOYALTY_PLAN_COMPANY
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'SUCCESS' AND ADD_JOURNEY = 'JOIN' THEN 1 END),0) AS JOIN_SUCCESS_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'FAILED' AND ADD_JOURNEY = 'JOIN' THEN 1 END),0) AS JOIN_FAILED_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'REQUEST' AND ADD_JOURNEY = 'JOIN' THEN 1 END),0) AS JOIN_PENDING_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'REMOVED' AND ADD_JOURNEY = 'JOIN' THEN 1 END),0) AS JOIN_REMOVED_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'SUCCESS' AND ADD_JOURNEY = 'LINK' THEN 1 END),0) AS LINK_SUCCESS_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'FAILED' AND ADD_JOURNEY = 'LINK' THEN 1 END),0) AS LINK_FAILED_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'REQUEST' AND ADD_JOURNEY = 'LINK' THEN 1 END),0) AS LINK_PENDING_STATE
        ,COALESCE(SUM(CASE WHEN EVENT_TYPE = 'REMOVED' AND ADD_JOURNEY = 'LINK' THEN 1 END),0) AS LINK_REMOVED_STATE
    FROM lc_start_end lc
    LEFT JOIN DIM_DATE d
        ON d.DATE >= lc.START_DATE
        AND d.DATE < lc.END_DATE
    GROUP BY
        d.DATE
        ,lc.BRAND
        ,lc.LOYALTY_PLAN_NAME
        ,lc.LOYALTY_PLAN_COMPANY
    HAVING
        DATE IS NOT NULL
        AND LOYALTY_PLAN_NAME IS NOT NULL
        AND BRAND IS NOT NULL
        AND (
            JOIN_SUCCESS_STATE != 0
            OR JOIN_FAILED_STATE != 0
            OR JOIN_PENDING_STATE != 0
            OR JOIN_REMOVED_STATE != 0
            OR LINK_SUCCESS_STATE != 0
            OR LINK_FAILED_STATE != 0
            OR LINK_PENDING_STATE != 0
            OR LINK_REMOVED_STATE != 0
        )
)

SELECT *
FROM count_up
