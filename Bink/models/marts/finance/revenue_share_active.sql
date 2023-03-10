/*
Created by:         Chris Mitchell
Created date:       2023-03-08
Last modified by:   
Last modified date: 

Description:
	Create table with calcs for revenue share Active Users For lloyds

Parameters:
    Ref FACT_TRANSACTION   FACT_USER

*/

WITH active_user AS (
    SELECT * FROM {{ref('src__fact_transaction')}}
),

active_user_channel AS (
    SELECT DISTINCT USER_ID, CHANNEL, EVENT_TYPE FROM {{ref('src__fact_user')}} WHERE EVENT_TYPE = 'CREATED' AND CHANNEL = 'LLOYDS'
),

active_user_stage as (
    SELECT
        DATE(DATE_TRUNC('month', t.EVENT_DATE_TIME)) AS DATE
        ,t.LOYALTY_ID
        ,t.USER_ID
        ,t.PROVIDER_SLUG AS MERCHANT
        ,uc.CHANNEL
    FROM active_user t
    INNER JOIN active_user_channel uc ON
        uc.USER_ID = t.USER_ID
),

active_user_count as (
    SELECT
        DATE
        ,MERCHANT
        ,CHANNEL
        ,COUNT(DISTINCT LOYALTY_ID) AS ACTIVE_USER
    FROM
        active_user_stage
    GROUP BY
        CHANNEL
        ,MERCHANT
        ,DATE
)

SELECT * FROM active_user_count ORDER BY DATE