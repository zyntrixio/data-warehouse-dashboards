-- For the dashboard, 'link' refers to lc_add_auth and lc_auth events
-- This stages a union of these tables

WITH
lc_status_change AS (
    SELECT *
    FROM {{ref('src__fact_lc_status_change')}}
)

,lc_add AS (
    SELECT *
    FROM {{ref('src__fact_lc_add')}}
)

,lc_status_lookup AS (
    SELECT *
    FROM {{ref('src__lookup_status_mapping')}}
)

,lc_links AS (
    SELECT
        sc.EVENT_DATE_TIME AS STATUS_EVENT_DATE_TIME
        ,a.EVENT_DATE_TIME AS ADD_LC_EVENT_DATE_TIME
        ,a.EVENT_TYPE
        ,a.AUTH_TYPE
        ,a.LOYALTY_CARD_ID
        ,a.LOYALTY_PLAN
        ,a.USER_ID
        ,CASE
            WHEN l.STATUS_ROLLUP = 'Success'
                THEN 'SUCCESS'
            WHEN l.STATUS_ROLLUP = 'Pending'
                THEN 'PENDING'
            WHEN l.STATUS_ROLLUP IS NULL
                THEN NULL
            ELSE 'FAILED'
            END AS STATUS_GROUPING
    FROM
        lc_status_change sc
    LEFT JOIN lc_add a
        ON a.LOYALTY_CARD_ID = sc.LOYALTY_CARD_ID
    LEFT JOIN lc_status_lookup l ON l.CODE = sc.TO_STATUS_ID
    WHERE
        a.AUTH_TYPE IN ('AUTH', 'ADD AUTH')
        AND sc.TO_STATUS_ID != sc.FROM_STATUS_ID
)

,dedupe AS (
    SELECT DISTINCT *
    FROM lc_links
)

SELECT *
FROM dedupe
