{{
    config(
        materialized='view',
        unique_key='account_id'
    )
}}

with deduplicated_accounts as (
    select distinct on (account_id)
        account_id,
        account_name,
        contact_email,
        registration_date
    from {{ ref('staging_business_accounts') }}
    order by account_id, registration_date desc
),

transaction_totals as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
)

select
    a.account_id,
    a.account_name,
    a.contact_email,
    a.registration_date,
    coalesce(t.total_transactions_amount, 0) as total_transactions_amount
from deduplicated_accounts a
left join transaction_totals t
    on a.account_id = t.account_id
order by a.registration_date desc;