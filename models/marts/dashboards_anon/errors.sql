WITH mock_brands AS (
    SELECT *
    FROM {{ref('trans__mock_brands')}}
)

,lc_sc AS (
    SELECT *
    FROM {{ref('src__fact_lc_status_change')}}
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,lc_lookup AS (
    SELECT *
    FROM {{ref('src__lookup_status_mapping')}}
)

,dim_date AS (
    SELECT *
    FROM {{ref('src__dim_date')}}
    WHERE
        DATE >= (SELECT MIN(EVENT_DATE_TIME) FROM lc_sc)
        AND DATE <= CURRENT_DATE()
)

,event_ordering AS (
    SELECT
        EVENT_DATE_TIME AS STATUS_START_TIME
        ,TO_STATUS_ID AS STATUS_ID
        ,TO_STATUS AS STATUS
        ,CHANNEL
        ,LOYALTY_CARD_ID
        ,USER_ID
        ,LEAD(EVENT_DATE_TIME, 1) OVER (PARTITION BY CHANNEL, LOYALTY_CARD_ID, USER_ID ORDER BY EVENT_DATE_TIME) AS STATUS_END_TIME
        ,LEAD(TO_STATUS_ID, 1) OVER (PARTITION BY CHANNEL, LOYALTY_CARD_ID, USER_ID ORDER BY EVENT_DATE_TIME) AS NEXT_STATUS_ID
        ,LAG(TO_STATUS_ID, 1 ) OVER (PARTITION BY CHANNEL, LOYALTY_CARD_ID, USER_ID ORDER BY EVENT_DATE_TIME) AS PREV_STATUS_ID
    FROM lc_sc
    -- QUALIFY
    --     (STATUS != 'Active' OR PREV_STATUS != 'Active') -- Ignore Active -> Active
    --     AND
    --     (STATUS != 'Pending' OR PREV_STATUS != 'Pending') -- Ignore Pending -> Pending
)

,join_status_types AS (
    SELECT
        lc.*
        ,lcl.STATUS_TYPE
        ,lcl_next.STATUS_TYPE AS NEXT_STATUS_TYPE
        -- ,lcl_prev.STATUS_TYPE AS PREV_STATUS_TYPE
    FROM event_ordering lc
    LEFT JOIN lc_lookup lcl
        ON lc.STATUS_ID = lcl.CODE
    LEFT JOIN lc_lookup lcl_prev
        ON lc.PREV_STATUS_ID = lcl_prev.CODE
    LEFT JOIN lc_lookup lcl_next
        ON lc.NEXT_STATUS_ID = lcl_next.CODE
    WHERE
        (lcl.STATUS_TYPE != 'Active' OR lcl_prev.STATUS_TYPE != 'Active') -- Ignore Active -> Active
        AND
        (lcl.STATUS_TYPE != 'Pending' OR lcl_prev.STATUS_TYPE != 'Pending') -- Ignore Pending -> Pending
)

,supplement AS (
    SELECT
    *
    ,CASE WHEN
        PREV_STATUS_ID IS NOT NULL AND PREV_STATUS_ID = STATUS_ID
        THEN TRUE
        ELSE FALSE
        END AS REPEATED_STATUS
    ,DATEDIFF(day, STATUS_START_TIME, STATUS_END_TIME) AS TIMEDIFF_DAYS
    ,DATEDIFF(hour, STATUS_START_TIME, STATUS_END_TIME) AS TIMEDIFF_HOURS
    ,DATEDIFF(min, STATUS_START_TIME, STATUS_END_TIME) AS TIMEDIFF_MINS
    ,DATEDIFF(sec, STATUS_START_TIME, STATUS_END_TIME) AS TIMEDIFF_SECONDS
    FROM join_status_types
)

SELECT * FROM supplement