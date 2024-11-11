{{
    config(
        materialized='table',
        unique_key='account_id',
        load_type='incremental'
    )
}}

with account_metrics as (
    select
        account_id,
        count(transaction_id) as total_number_of_transactions,
        {{ calculate_total_transactions('transaction_amount') }} as total_transaction_volume,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
)

, prepared_accounts as (
    select
        ia.account_id,
        ia.account_name,
        ia.contact_email,
        ia.registration_date,
        am.total_number_of_transactions,
        am.total_transaction_volume,
        am.first_transaction_date,
        am.last_transaction_date,
        {{ set_audit_columns() }}
    from {{ ref('integrated_accounts') }} ia
    left join account_metrics am on ia.account_id = am.account_id
)

select *
from prepared_accounts;