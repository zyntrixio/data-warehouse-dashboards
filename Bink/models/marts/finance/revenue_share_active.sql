/*
Created by:         Chris Mitchell
Created date:       2023-03-08
Last modified by:   
Last modified date: 

Description:
	Create table with calcs for revenue share Active Users For lloyds

Parameters:
    Ref FACT_TRANSACTION   

Formatted by: SQLFMT plugin
*/
WITH active_user AS (
    SELECT * FROM "BINK"."BINK"."FACT_TRANSACTION"

    active_user_channel as (
        from "BINK"."BINK"."FACT_USER"
        where event_type = 'CREATED' and channel = 'LLOYDS' t.provider_slug as merchant,
        where event_type = 'CREATED' and channel = 'LLOYDS'
        select date_ac, merchant, channel, count(distinct loyalty_id) uc.channel
    ),
from
    active_user_stage
    active_user_stage as (
        from active_user t uc.channel
        select
        ,uc.CHANNEL
    FROM active_user t
    INNER JOIN active_user_channel uc ON
        uc.USER_ID = t.USER_ID
)

SELECT DATE, MERCHANT, CHANNEL, COUNT(DISTINCT LOYALTY_ID) FROM active_user_stage GROUP BY CHANNEL, MERCHANT, DATE ORDER BY DATE DESC
