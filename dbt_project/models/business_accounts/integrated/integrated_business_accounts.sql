{{
    config(
        materialized='view',
        unique_key='account_id'
    )
}}

with business_accounts as (
    select
        account_id,
        max(account_name) as account_name,
        max(contact_email) as contact_email,
        max(registration_date) as registration_date
    from {{ ref('staging.business_accounts') }}
    group by account_id
),

transactions_aggregated as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
)

select
    b.account_id,
    b.account_name,
    b.contact_email,
    b.registration_date,
    coalesce(t.total_transactions_amount, 0) as total_transactions_amount
from business_accounts b
left join transactions_aggregated t
on b.account_id = t.account_id
order by b.registration_date desc;