{{ config(alias='user_status') }}

WITH user_events AS (
    SELECT *
    FROM {{ref('trans__user_events')}}
    WHERE EVENT NOT IN ('LC_REGISTER', 'LC_REMOVE')
    AND BRAND IS NOT NULL
)

,add_history_columns AS ( -- Calculate previous and following refresh and transaction times
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
        AND EVENT != 'DELETE'
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
        AND EVENT != 'DELETE'
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
        ,EVENT_DATE_TIME::DATE AS STATUS_FROM_DATE
        ,EVENT
        ,BRAND
        ,CASE
            WHEN EVENT = 'STATUS_CHANGE_DORMANT'
            THEN 'DORMANT'
            WHEN EVENT = 'STATUS_CHANGE_INACTIVE'
            THEN 'INACTIVE'
            WHEN EVENT = 'TRANSACT'
            THEN 'ACTIVE'
            WHEN EVENT = 'CREATE'
            THEN 'REGISTRATION'
            WHEN EVENT = 'REFRESH' AND COALESCE(DAYS_SINCE_LAST_TRANSACT_EVENT,31) > 30
            THEN 'INACTIVE'
            WHEN EVENT = 'REFRESH' AND COALESCE(DAYS_SINCE_LAST_TRANSACT_EVENT,31) <= 30
            THEN 'ACTIVE'
            WHEN EVENT = 'DELETE'
            THEN 'REMOVED'
            END AS STATUS
    FROM add_status_change_events
    WHERE STATUS_FROM_DATE <= CURRENT_DATE()
)

,day_ends AS ( -- just get events that finish the day
    SELECT
        *
        ,MAX(EVENT_DATE_TIME) OVER (PARTITION BY USER_ID, STATUS_FROM_DATE) AS LAST_EVENT_DAILY
    FROM calculate_statuses
    QUALIFY
        EVENT_DATE_TIME = LAST_EVENT_DAILY
)

,select_status_changes AS ( -- Just get events for which there is a status change. Calculate Registration & Deletion dates date
    SELECT
        USER_ID
        ,STATUS_FROM_DATE
        ,BRAND
        ,STATUS
        ,CASE
            WHEN STATUS != 'REGISTRATION' THEN LAG(STATUS) OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME) 
            END AS PREVIOUS_STATUS
        ,STATUS_FROM_DATE - MAX(CASE WHEN EVENT = 'CREATE' THEN EVENT_DATE_TIME END)
            OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::DATE AS DAYS_SINCE_REGISTRATION
        ,STATUS_FROM_DATE - MAX(CASE WHEN EVENT = 'DELETE' THEN EVENT_DATE_TIME END)
            OVER (PARTITION BY USER_ID ORDER BY EVENT_DATE_TIME ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)::DATE AS DAYS_SINCE_DELETION
    FROM
        day_ends
    QUALIFY
        (STATUS != PREVIOUS_STATUS
        OR PREVIOUS_STATUS IS NULL)
        AND
        (DAYS_SINCE_DELETION IS NULL
        OR DAYS_SINCE_REGISTRATION < DAYS_SINCE_DELETION)
)

,calculate_status_times AS (
    SELECT
        USER_ID
        ,STATUS_FROM_DATE
        ,LEAD(STATUS_FROM_DATE) OVER (PARTITION BY USER_ID ORDER BY STATUS_FROM_DATE) AS STATUS_TO_DATE
        ,BRAND
        ,STATUS AS CURRENT_STATUS
        ,COALESCE(LEAD(STATUS_FROM_DATE) OVER (PARTITION BY USER_ID ORDER BY STATUS_FROM_DATE), CURRENT_DATE()) - STATUS_FROM_DATE AS DAYS_IN_CURRENT_STATUS
        ,PREVIOUS_STATUS
        ,CASE
            WHEN PREVIOUS_STATUS IS NOT NULL THEN STATUS_FROM_DATE - LAG(STATUS_FROM_DATE) OVER (PARTITION BY USER_ID ORDER BY STATUS_FROM_DATE)
            END AS DAYS_IN_PREVIOUS_STATUS
        ,DAYS_SINCE_REGISTRATION
    FROM
        select_status_changes
)

SELECT *
FROM calculate_status_times