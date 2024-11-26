{{
    config(
        materialized='view',
        unique_key='account_id'
    )
}}

with business_accounts_staging as (
    select
        account_id,
        business_name as account_name,
        contact_email,
        to_date(registration_date, 'YYYY-MM-DD') as registration_date
    from {{ source('raw', 'raw_business_accounts') }}
    where contact_email is not null
),

deduplicated_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date,
        row_number() over (partition by account_id order by registration_date desc) as rn
    from business_accounts_staging
),

latest_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date
    from deduplicated_accounts
    where rn = 1
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
        la.account_id,
        la.account_name,
        la.contact_email,
        la.registration_date,
        coalesce(tt.total_transactions_amount, 0) as total_transactions_amount
    from latest_accounts la
    left join transaction_totals tt
    on la.account_id = tt.account_id
)

select *
from integrated_accounts
order by registration_date desc;