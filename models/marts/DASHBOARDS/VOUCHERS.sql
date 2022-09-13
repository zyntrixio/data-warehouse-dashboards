with VOUCHERS_NON_ANON as (
    select * from 
    {{ref('VOUCHERS_NON_ANON')}}
)


, num as (
    select 
        loyalty_card_id,
        user_id,
        loyalty_plan_id,
        voucher_code,
        loyalty_plan_company,
        loyalty_plan_slug,
        loyalty_plan_tier,
        loyalty_plan_name_card,
        loyalty_plan_name,
        loyalty_plan_category, 
        state,
        date_redeemed,
        date_issued,
        expiry_date,
        redemption_tracked,
        time_to_redemption,
        days_left_on_vouchers,
        days_valid_for,
        issued,
        issued_channel,
        redemed,
        redeemed_channel
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
            loyalty_plan_id ,
            state,
            date_redeemed,
            date_issued,
            expiry_date,
            redemption_tracked,
            time_to_redemption,
            days_left_on_vouchers,
            days_valid_for,
            issued,
            issued_channel,
            redemed,
            redeemed_channel
    from num
)

select *
from anon