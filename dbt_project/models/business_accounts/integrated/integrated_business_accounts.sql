{
    config(
        materialized='view',
        unique_key='account_id'
    )
}

with deduplicated_accounts as (
    select
        account_id,
        max(account_name) as account_name,
        max(contact_email) as contact_email,
        max(registration_date) as registration_date
    from (
        select
            account_id,
            account_name,
            contact_email,
            registration_date
        from {{ ref('staging.business_accounts') }}
    ) sub
    group by account_id
),
transaction_amounts as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),
enriched_accounts as (
    select
        a.account_id,
        a.account_name,
        a.contact_email,
        a.registration_date,
        coalesce(t.total_transactions_amount, 0) as total_transactions_amount
    from deduplicated_accounts a
    left join transaction_amounts t
    on a.account_id = t.account_id
)

select
    account_id,
    account_name,
    contact_email,
    registration_date,
    total_transactions_amount
from enriched_accounts
order by registration_date desc;