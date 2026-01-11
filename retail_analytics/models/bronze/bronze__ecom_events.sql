with src as (

    select *
    from {{ source('raw', 'ecom_events_raw') }}

),

typed as (

    select
        try_to_timestamp_ntz(nullif(trim(event_time), '')) as event_ts,
        nullif(trim(event_type), '')                      as event_type,

        try_to_number(nullif(trim(product_id), ''))       as product_id,
        nullif(trim(category_id), '')                     as category_id,
        nullif(trim(category_code), '')                   as category_code,
        nullif(trim(brand), '')                           as brand,

        try_to_decimal(nullif(trim(price), ''), 18, 2)    as price,

        try_to_number(nullif(trim(user_id), ''))          as user_id,
        nullif(trim(user_session), '')                    as user_session

    from src

),

final as (

    select *
    from typed

)

select * from final