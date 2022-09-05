WITH mock_brands AS (
    SELECT *
    FROM {{ref('trans__mock_brands')}}
)

,lc_register AS (
    SELECT *
    FROM {{ref('trans__lc_registered')}}
)

,lc_removed AS (
    SELECT *
    FROM {{ref('src__fact_lc_removed')}}
)

,users AS (
    SELECT *
    FROM {{ref('src__fact_user')}} 
)

,dim_lc AS (
    SELECT *
    FROM {{ref('src__dim_loyalty_card')}}
)

,dim_date AS (
    SELECT *
    FROM {{ref('src__dim_date')}}
)

,user_timespan as (
    SELECT
        uc.USER_ID
        ,DATE(uc.EVENT_DATE_TIME) AS U_CREATED_DATE
        ,DATE(UD.EVENT_DATE_TIME) AS U_DELETED_DATE 
    FROM
        users uc
    LEFT JOIN users ud
        ON uc.USER_ID = ud.USER_ID
        AND ud.EVENT_TYPE = 'DELETED'
    WHERE
        uc.EVENT_TYPE = 'CREATED'
)

,lc_start_end as (
    SELECT
        lcj.LOYALTY_CARD_ID
        ,lcj.USER_ID
        ,DATE(lcj.EVENT_DATE_TIME) AS LC_START_DATE
        ,DATE(lcr.EVENT_DATE_TIME) AS LC_END_DATE
    FROM lc_register lcj
    LEFT JOIN lc_removed lcr
        ON lcj.LOYALTY_CARD_ID  = lcr.LOYALTY_CARD_ID
    WHERE
        EVENT_TYPE = 'SUCCESS'
)

,users_start_end AS (
    select
        u.USER_ID
        ,lc.LC_START_DATE AS START_DATE
        ,LEAST(COALESCE(lc.LC_END_DATE, '9999-01-01'::DATE), COALESCE(u.U_DELETED_DATE, '9999-01-01'::DATE)) AS END_DATE
        ,b.BRAND
        ,dlc.LOYALTY_PLAN_NAME
    FROM user_timespan u
    LEFT JOIN lc_start_end lc
        ON u.USER_ID = lc.USER_ID
        AND lc.LC_START_DATE >= u.U_CREATED_DATE
    LEFT JOIN mock_brands b
        ON u.USER_ID = b.USER_ID
    LEFT JOIN dim_lc dlc
        ON dlc.LOYALTY_CARD_ID = lc.LOYALTY_CARD_ID
    WHERE lc.USER_ID IS NOT NULL
)

      
,u_creations as (
    SELECT
        START_DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
        ,COUNT(*) C
    FROM
        users_start_end
    GROUP BY
        START_DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
)

,u_deletions as (
    SELECT
        END_DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
        ,COUNT(*) C
    FROM
        users_start_end
    WHERE
        END_DATE != '9999-01-01'::DATE
    GROUP BY
        END_DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
)
  
,date_range AS (
    SELECT
        DATE
    FROM
        dim_date
    WHERE
        DATE >= (SELECT MIN(START_DATE) FROM users_start_end)
        AND DATE <= (SELECT MAX(END_DATE) FROM users_start_end WHERE END_DATE != '9999-01-01'::DATE)
)
  
,union_matching_records AS (
    SELECT
        START_DATE AS DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
    FROM u_creations
    UNION
    SELECT
        END_DATE AS DATE
        ,BRAND
        ,LOYALTY_PLAN_NAME
    FROM u_deletions
) 

,count_up as (
    SELECT
        d.DATE
        ,r.BRAND
        ,r.LOYALTY_PLAN_NAME
        ,COALESCE(uc.C,0) AS LC_USERS_CREATED
        ,COALESCE(ud.C,0) AS LC_USERS_DELETED
        ,LC_USERS_CREATED - LC_USERS_DELETED AS DAILY_CHANGE_IN_LC_USERS
        ,SUM(DAILY_CHANGE_IN_LC_USERS) OVER (PARTITION BY r.BRAND, r.LOYALTY_PLAN_NAME ORDER BY d.DATE ASC) AS TOTAL_LC_USERS_COUNT
    FROM date_range d
    LEFT JOIN union_matching_records r
        ON d.DATE = r.DATE
    LEFT JOIN u_creations uc
        ON d.DATE = uc.START_DATE
            AND r.BRAND = uc.BRAND
            AND r.LOYALTY_PLAN_NAME = uc.LOYALTY_PLAN_NAME
    LEFT JOIN u_deletions ud
        ON d.DATE = ud.END_DATE
            AND r.BRAND = ud.BRAND
            AND r.LOYALTY_PLAN_NAME = ud.LOYALTY_PLAN_NAME
)

SELECT *
FROM count_up
ORDER BY
    DATE
    ,BRAND
    ,LOYALTY_PLAN_NAME
