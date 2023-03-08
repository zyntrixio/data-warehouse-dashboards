/*

Created by:         Chris Mitchell
Created date:       2023-03-08
Last modified by:   
Last modified date: 

Description:
	Create table with calcs for revenue share views

Parameters:
    ref_object      

*/

WITH joins AS (
    SELECT *
    FROM "BINK"."BINK"."FACT_LOYALTY_CARD_ADD"
)

,select_joins AS (
     SELECT
        DATE_TRUNC('month', EVENT_DATE_TIME) AS REPORT_MONTH
        ,AUTH_TYPE
        ,CHANNEL
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN_NAME
     FROM
        joins
     WHERE AUTH_TYPE IN ('JOIN', 'REGISTER') 
     AND EVENT_TYPE = 'SUCCESS'
)

,count_joins AS (
    SELECT
        REPORT_MONTH
        ,CHANNEL
        ,LOYALTY_PLAN_NAME
        ,COUNT(DISTINCT LOYALTY_CARD_ID) 
    FROM select_joins
    GROUP BY
        CHANNEL
        ,LOYALTY_PLAN_NAME
        ,REPORT_MONTH
)

SELECT * FROM count_joins ORDER BY REPORT_MONTH, CHANNEL, LOYALTY_PLAN_NAME