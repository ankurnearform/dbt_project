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
    select
        account_id,
        account_name,
        contact_email,
        registration_date,
        row_number() over (partition by account_id order by registration_date desc) as rn
    from business_accounts
),
filtered_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date
    from deduplicated_accounts
    where rn = 1
),
transactions as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),
integrated_accounts as (
    select
        fa.account_id,
        fa.account_name,
        fa.contact_email,
        fa.registration_date,
        coalesce(t.total_transactions_amount, 0) as total_transactions_amount
    from filtered_accounts fa
    left join transactions t
    on fa.account_id = t.account_id
)
select *
from integrated_accounts
order by registration_date desc;