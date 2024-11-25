{{
    config(
        materialized='view',
        unique_key='account_id'
    )
}}

with business_accounts as (
    select
        account_id,
        business_name as account_name,
        contact_email,
        to_date(registration_date, 'YYYY-MM-DD') as registration_date
    from {{ source('raw', 'raw_business_accounts') }}
    where contact_email is not null
),

deduplicated_accounts as (
    select distinct on (account_id)
        account_id,
        account_name,
        contact_email,
        registration_date
    from business_accounts
    order by account_id, registration_date desc
),

transaction_totals as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),

integrated_accounts as (
    select
        da.account_id,
        da.account_name,
        da.contact_email,
        da.registration_date,
        coalesce(tt.total_transactions_amount, 0) as total_transactions_amount
    from deduplicated_accounts da
    left join transaction_totals tt
    on da.account_id = tt.account_id
    order by da.registration_date desc
)

select *
from integrated_accounts;