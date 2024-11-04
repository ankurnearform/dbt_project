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
        max(registration_date_formatted) as registration_date_formatted
    from {{ ref('staging.business_accounts') }}
    group by account_id
),

transactions_amount as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
)

select
    ba.account_id,
    ba.account_name,
    ba.contact_email,
    ba.registration_date_formatted,
    coalesce(ta.total_transactions_amount, 0) as total_transactions_amount
from business_accounts ba
left join transactions_amount ta
    on ba.account_id = ta.account_id
order by ba.registration_date_formatted desc;