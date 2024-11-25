{{
    config(
        materialized='table',
        unique_key='account_id',
        load_type='incremental'
    )
}}

with business_accounts as (
    select
        account_id,
        max(account_name) as account_name,
        max(contact_email) as contact_email,
        max(registration_date) as registration_date
    from {{ ref('integrated.business_accounts') }}
    group by account_id
),

transactions_aggregated as (
    select
        account_id,
        count(*) as total_transactions,
        {{ calculate_total_transactions('transaction_amount') }} as total_transactions_volume,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
)

select
    b.account_id,
    b.account_name,
    b.contact_email,
    b.registration_date,
    coalesce(t.total_transactions, 0) as total_transactions,
    coalesce(t.total_transactions_volume, 0) as total_transactions_volume,
    t.first_transaction_date,
    t.last_transaction_date,
    {{ set_audit_columns() }}
from business_accounts b
left join transactions_aggregated t
on b.account_id = t.account_id
order by b.registration_date desc;