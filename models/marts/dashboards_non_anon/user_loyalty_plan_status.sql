WITH user_events AS (
    SELECT *
    FROM {{ref('trans__user_events')}}
    WHERE EVENT NOT IN ('CREATE', 'DELETE')
)

,add_history_columns AS ( -- Calculate previous and following refresh and transaction times
    SELECT
        *
        ,EVENT_DATE_TIME::DATE - MAX(CASE WHEN EVENT = 'REFRESH' THEN EVENT_DATE_TIME END)
            OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::DATE
            AS DAYS_SINCE_LAST_REFRESH_EVENT
        ,EVENT_DATE_TIME::DATE - MAX(CASE WHEN EVENT = 'TRANSACT' THEN EVENT_DATE_TIME END)
            OVER (PARTITION BY USER_ID, LOYALTY_PLAN ORDER BY EVENT_DATE_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::DATE
            AS DAYS_SINCE_LAST_TRANSACT_EVENT
        ,MIN(CASE WHEN EVENT = 'REFRESH' THEN EVENT_DATE_TIME END)
            OVER (PARTITION BY USER_ID, LOYALTY_PLAN ORDER BY EVENT_DATE_TIME ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)::DATE - EVENT_DATE_TIME::DATE
            AS NEXT_REFRESH_EVENT
        ,MIN(CASE WHEN EVENT = 'TRANSACT' THEN EVENT_DATE_TIME END)
            OVER (PARTITION BY USER_ID, LOYALTY_PLAN ORDER BY EVENT_DATE_TIME ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)::DATE - EVENT_DATE_TIME::DATE
            AS NEXT_TRANSACT_EVENT
    FROM
        user_events
)

,inactive_events AS ( -- occurs when it's been 30 days since a transaction, but less than 30 since a refresh
    SELECT
        USER_ID
        ,DATEADD(day, 30, EVENT_DATE_TIME) AS EVENT_DATE_TIME
        ,'STATUS_CHANGE_INACTIVE' AS EVENT
        ,LOYALTY_PLAN
        ,BRAND
        ,NULL AS DAYS_SINCE_LAST_REFRESH_EVENT
        ,NULL AS DAYS_SINCE_LAST_TRANSACT_EVENT
        ,NULL AS NEXT_REFRESH_EVENT
        ,NULL AS NEXT_TRANSACT_EVENT
    FROM 
        add_history_columns
    WHERE
        COALESCE(NEXT_REFRESH_EVENT,31) <= 30
        AND COALESCE(NEXT_TRANSACT_EVENT,31) > 30
)

,dormant_events AS ( -- occurs when it's been 30 days since a transaction or refresh
    SELECT
        USER_ID
        ,DATEADD(day, 30, EVENT_DATE_TIME) AS EVENT_DATE_TIME
        ,'STATUS_CHANGE_DORMANT' AS EVENT
        ,LOYALTY_PLAN
        ,BRAND
        ,NULL AS DAYS_SINCE_LAST_REFRESH_EVENT
        ,NULL AS DAYS_SINCE_LAST_TRANSACT_EVENT
        ,NULL AS NEXT_REFRESH_EVENT
        ,NULL AS NEXT_TRANSACT_EVENT
    FROM 
        add_history_columns
    WHERE
        COALESCE(NEXT_REFRESH_EVENT,31) > 30
        AND COALESCE(NEXT_TRANSACT_EVENT,31) > 30
)

,add_status_change_events AS ( -- Union in status change events
    SELECT * FROM add_history_columns
    UNION ALL
    SELECT * FROM inactive_events
    UNION ALL
    SELECT * FROM dormant_events
)

,calculate_statuses AS ( -- Calculate status for each event. May have status duplication
    SELECT
        USER_ID
        ,EVENT_DATE_TIME
        ,EVENT
        ,LOYALTY_PLAN
        ,BRAND
        ,CASE
            WHEN EVENT = 'STATUS_CHANGE_DORMANT'
            THEN 'DORMANT'
            WHEN EVENT = 'STATUS_CHANGE_INACTIVE'
            THEN 'INACTIVE'
            WHEN EVENT = 'TRANSACT'
            THEN 'ACTIVE'
            WHEN EVENT = 'LC_REGISTER'
            THEN 'REGISTRATION'
            WHEN EVENT = 'REFRESH' AND COALESCE(DAYS_SINCE_LAST_TRANSACT_EVENT,31) > 30
            THEN 'INACTIVE'
            WHEN EVENT = 'REFRESH' AND COALESCE(DAYS_SINCE_LAST_TRANSACT_EVENT,31) <= 30
            THEN 'ACTIVE'
            END AS STATUS
        -- ,DAYS_SINCE_LAST_REFRESH_EVENT
        -- ,DAYS_SINCE_LAST_TRANSACT_EVENT
        -- ,NEXT_REFRESH_EVENT
        -- ,NEXT_TRANSACT_EVENT
    FROM add_status_change_events
)

SELECT *
FROM calculate_statuses