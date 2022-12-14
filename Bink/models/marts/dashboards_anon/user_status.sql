WITH user_statuses AS (
    SELECT *
    FROM {{ref('src__fact_user')}}
)

,count_up AS (
    SELECT
        DATE(EVENT_DATE_TIME) AS DATE
        ,BRAND
        ,COALESCE(COUNT(CASE WHEN EVENT_TYPE = 'CREATED' THEN 1 END) ,0) AS DAILY_REGISTRATIONS
        ,COALESCE(COUNT(CASE WHEN EVENT_TYPE = 'DELETED' THEN 1 END) ,0) AS DAILY_DEREGISTRATIONS
    FROM user_statuses
    GROUP BY
        DATE
        ,BRAND
    HAVING
        DATE IS NOT NULL
        AND BRAND IS NOT NULL
)

SELECT *
FROM count_up
