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

transactions_aggregated as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),

integrated_business_accounts as (
    select
        da.account_id,
        da.account_name,
        da.contact_email,
        da.registration_date,
        coalesce(ta.total_transactions_amount, 0) as total_transactions_amount
    from deduplicated_accounts da
    left join transactions_aggregated ta
    on da.account_id = ta.account_id
)

select *
from integrated_business_accounts
order by registration_date desc;