{{
    config(
        materialized='table',
        unique_key='account_id',
        load_type='incremental'
    )
}}

with account_transactions as (
    select
        account_id,
        count(*) as total_number_of_transactions,
        {{ calculate_total_transactions('transaction_amount') }} as total_transaction_volume,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),

prepared_accounts as (
    select
        ia.account_id,
        ia.account_name,
        ia.contact_email,
        ia.registration_date,
        at.total_number_of_transactions,
        at.total_transaction_volume,
        at.first_transaction_date,
        at.last_transaction_date
    from {{ ref('integrated_accounts') }} ia
    left join account_transactions at
    on ia.account_id = at.account_id
)

select
    account_id,
    account_name,
    contact_email,
    registration_date,
    total_number_of_transactions,
    total_transaction_volume,
    first_transaction_date,
    last_transaction_date,
    {{ set_audit_columns() }}
from prepared_accounts;