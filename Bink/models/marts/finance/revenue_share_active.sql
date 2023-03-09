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
{{
    config(
        materialized="table",
    )
}}

with
    active_user as (select * from "BINK"."BINK"."FACT_TRANSACTION"),
    active_user_channel as (
        select distinct user_id, channel, event_type
        from "BINK"."BINK"."FACT_USER"
        where event_type = 'CREATED' and channel = 'LLOYDS'
    ),
    active_user_stage as (
        select
            date(date_trunc('month', t.event_date_time)) as date,
            t.loyalty_id,
            t.user_id,
            t.provider_slug as merchant,
            uc.channel
        from active_user t
        inner join active_user_channel uc on uc.user_id = t.user_id
    )

select date, merchant, channel, count(distinct loyalty_id)
from active_user_stage
group by channel, merchant, date
order by date desc
;
