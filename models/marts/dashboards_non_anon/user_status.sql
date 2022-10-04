WITH user_events AS (
    SELECT *
    FROM {{ref('trans__user_events')}}
    WHERE LOYALTY_PLAN IS NOT NULL
    AND BRAND IS NOT NULL
)

,add_history_columns AS (
    SELECT
        USER_ID
        ,EVENT_DATE_TIME
        ,EVENT
        ,BRAND
        ,EVENT_DATE_TIME::DATE - MAX(CASE WHEN EVENT = 'TRANSACT' THEN EVENT_DATE_TIME END)
            OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::DATE
            AS DAYS_SINCE_LAST_TRANSACT_EVENT
        ,MIN(CASE WHEN EVENT = 'REFRESH' THEN EVENT_DATE_TIME END)
            OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)::DATE - EVENT_DATE_TIME::DATE
            AS NEXT_REFRESH_EVENT
        ,MIN(CASE WHEN EVENT = 'TRANSACT' THEN EVENT_DATE_TIME END)
            OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)::DATE - EVENT_DATE_TIME::DATE
            AS NEXT_TRANSACT_EVENT
    FROM
        user_events
)

,inactive_events AS ( -- occurs when it's been 30 days since a transaction, but less than 30 since a refresh
    SELECT
        USER_ID
        ,DATEADD(day, 30, EVENT_DATE_TIME) AS EVENT_DATE_TIME
        ,'STATUS_CHANGE_INACTIVE' AS EVENT
        ,BRAND
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
        ,BRAND
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

,calculate_statuses AS ( -- Calculate status for each event. May have status duplication. Also removes hypothetical future events
    SELECT
        USER_ID
        ,EVENT_DATE_TIME
        ,EVENT_DATE_TIME::DATE AS EVENT_DATE
        ,EVENT
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
    FROM add_status_change_events
    WHERE EVENT_DATE <= CURRENT_DATE()
)

,day_ends AS ( -- just get events that finish the day. TWEAK TO INCLUDE MOST DOMINANT EVENT
    SELECT
        *
        ,MAX(EVENT_DATE_TIME) OVER (PARTITION BY USER_ID, EVENT_DATE) AS LAST_EVENT_DAILY
    FROM calculate_statuses
    QUALIFY
        EVENT_DATE_TIME = LAST_EVENT_DAILY
)

,select_status_changes AS ( -- just get events for which there is a status change
    SELECT
        USER_ID
        ,EVENT_DATE
        ,BRAND
        ,STATUS
        ,LAG(STATUS) OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME) AS PREVIOUS_STATUS
    FROM
        day_ends
    QUALIFY
        STATUS != PREVIOUS_STATUS
        OR PREVIOUS_STATUS IS NULL
)

,calculate_status_times AS (
    SELECT
        USER_ID
        ,EVENT_DATE
        ,BRAND
        ,STATUS AS CURRENT_STATUS
        ,COALESCE(LEAD(EVENT_DATE) OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE), CURRENT_DATE()) - EVENT_DATE AS DAYS_IN_CURRENT_STATUS
        ,PREVIOUS_STATUS
        ,EVENT_DATE - LAG(EVENT_DATE) OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE) AS DAYS_IN_PREVIOUS_STATUS
    FROM
        select_status_changes
)

SELECT *
FROM calculate_status_times