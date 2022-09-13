with FACT_VOUCHER AS (
    SELECT * 
    FROM {{ref('src__FACT_VOUCHER')}}

)


, DIM_LOYALTY_CARD AS (
    SELECT * 
    FROM {{ref('src_DIM_LOYALTY_CARD')}}

)

, voucher_table as (
    select 
        d.loyalty_plan_company,
        d.loyalty_plan_slug,
        d.loyalty_plan_tier,
        d.loyalty_plan_name_card,
        d.loyalty_plan_name,
        d.loyalty_plan_category,
        f.voucher_code,
        f.loyalty_card_id,
        d.loyalty_plan_id,
        f.current_channel,
        f.user_id,
        f.earn_type,
        f.state,
        f.date_redeemed,
        f.date_issued,
        f.expiry_date,
        f.redemption_tracked,
        f.time_to_redemption,
        f.days_left_on_vouchers,
        f.days_valid_for,
        f.issued,
        f.issued_channel,
        f.redemed,
        f.redeemed_channel
    from FACT_VOUCHER f
    left join DIM_LOYALTY_CARD D 
    on f.loyalty_card_id = d.loyalty_card_id
)


select * 
FROM voucher_table