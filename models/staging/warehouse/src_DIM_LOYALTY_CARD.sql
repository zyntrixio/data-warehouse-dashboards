with source as (

    select * from {{ source('BINK', 'DIM_LOYALTY_CARD') }}

),

renamed as (

    select
        loyalty_card_id,
        add_auth_status,
        add_auth_date_time,
        join_status,
        join_date_time,
        register_status,
        register_date_time,
        updated,
        status_id,
        status,
        status_type,
        status_rollup,
        link_date,
        created,
        orders,
        originating_journey,
        is_deleted,
        loyalty_plan_id,
        loyalty_plan_company,
        loyalty_plan_slug,
        loyalty_plan_tier,
        loyalty_plan_name_card,
        loyalty_plan_name,
        loyalty_plan_category_id,
        loyalty_plan_category

    from source

)

select * from renamed