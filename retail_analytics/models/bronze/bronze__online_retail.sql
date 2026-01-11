with src as (

    select *
    from {{ source('raw', 'online_retail_ii_raw') }}

),

typed as (

    select
        nullif(trim(invoiceno), '')                         as invoice_no,
        nullif(trim(stockcode), '')                         as stock_code,
        nullif(trim(description), '')                       as description,

        try_to_number(nullif(trim(quantity), ''))           as quantity,
        try_to_timestamp_ntz(nullif(trim(invoicedate), '')) as invoice_ts,
        try_to_decimal(nullif(trim(unitprice), ''), 18, 4)  as unit_price,

        try_to_number(nullif(trim(customerid), ''))         as customer_id,
        nullif(trim(country), '')                           as country

    from src

),

final as (

    select *
    from typed
    where invoice_no is not null

)

select * from final