/*
Created by:         Chris Mitchell
Created date:       2023-03-08
Last modified by:   
Last modified date: 

Description:
	Create table with calcs for revenue share JOINS and REG users lloyds

Parameters:
    REF FACT_LOYALTY_CARD_ADD

*/

WITH
    joins AS (
        SELECT *
        FROM {{ref('src__fact_lc_add')}}
        WHERE channel = 'LLOYDS'
    )

    ,select_joins AS (
        SELECT
            date_trunc('month', event_date_time) AS report_month
            ,auth_type
            ,channel
            ,event_type
            ,loyalty_card_id
            ,loyalty_plan_name
        FROM joins
        WHERE auth_type
        IN ('JOIN', 'REGISTER')
        AND event_type = 'SUCCESS'
    )
    ,count_joins AS (
        SELECT
            report_month
            ,channel
            ,loyalty_plan_name
            ,count(distinct loyalty_card_id)
        FROM select_joins
        GROUP BY
            channel
            ,loyalty_plan_name
            ,report_month
    )

SELECT *
FROM count_joins
ORDER BY report_month, channel, loyalty_plan_name