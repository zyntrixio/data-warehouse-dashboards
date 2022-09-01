with VOUCHERS_NON_ANON as (
    select * from 
    {{ref('VOUCHERS_NON_ANON')}}
)


, num as (
    select 
        loyalty_plan_company,
        loyalty_plan_slug,
        loyalty_plan_tier,
        loyalty_plan_name_card,
        loyalty_plan_name,
        loyalty_plan_category,
        voucher_code,
        loyalty_card_id,
        loyalty_plan_id,
        state,
        voucher_type,
        date_redeemed,
        date_issued,
        expiry_date,
        redemption_tracked,
        time_to_redemption,
        days_left_on_vouchers,
        days_valid_for,
        UNIFORM( 1 ,50 , random() ) as numb
    from VOUCHERS_NON_ANON
)

, anon as (
    select loyalty_plan_company,
            loyalty_plan_slug,
            loyalty_plan_tier,
            loyalty_plan_name_card,
            loyalty_plan_name,
            loyalty_plan_category,
            md5(voucher_code) voucher_code ,
            md5(loyalty_card_id) as loyalty_card_id,
            md5(loyalty_plan_id) loyalty_plan_id ,
            state,
            voucher_type,
            case when date_redeemed  is not null then  dateadd(day, numb, date_redeemed) 
                else null
                end as date_redeemed  ,
            case when date_issued  is not null then  dateadd(day, numb, date_issued ) 
                else null
                end as date_issued  ,
            case when expiry_date is not null then dateadd(day, numb, expiry_date )
                else null
                end as expiry_date ,
            redemption_tracked ,
            case when time_to_redemption is not null then  time_to_redemption + numb
                else null
                end as time_to_redemption ,
            case when days_left_on_vouchers is not null then days_left_on_vouchers + numb
                else null
                end as days_left_on_vouchers ,
            case when days_valid_for is not null then days_valid_for + numb
                else null
                end as days_valid_for
    from num
)

select *
from anon