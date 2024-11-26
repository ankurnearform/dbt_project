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
        count(*) as total_number_of_transactions,
        {{ calculate_total_transactions('transaction_amount') }} as total_transaction_volume,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),

prepared_business_accounts as (
    select
        iba.account_id,
        iba.account_name,
        iba.contact_email,
        iba.registration_date,
        am.total_number_of_transactions,
        am.total_transaction_volume,
        am.first_transaction_date,
        am.last_transaction_date
    from {{ ref('integrated_business_accounts') }} iba
    left join account_metrics am on iba.account_id = am.account_id
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
from prepared_business_accounts;