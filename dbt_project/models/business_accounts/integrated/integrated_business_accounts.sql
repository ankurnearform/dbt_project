{{
    config(
        materialized='view',
        unique_key='account_id'
    )
}}

with deduplicated_accounts as (
    select
        account_id,
        max(account_name) as account_name,
        max(contact_email) as contact_email,
        max(registration_date) as registration_date
    from {{ ref('staging.business_accounts') }}
    group by account_id
),
transaction_totals as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),
enriched_accounts as (
    select
        da.account_id,
        da.account_name,
        da.contact_email,
        da.registration_date,
        coalesce(tt.total_transactions_amount, 0) as total_transactions_amount
    from deduplicated_accounts da
    left join transaction_totals tt
    on da.account_id = tt.account_id
)
select
    account_id,
    account_name,
    contact_email,
    registration_date,
    total_transactions_amount
from enriched_accounts
order by registration_date desc;