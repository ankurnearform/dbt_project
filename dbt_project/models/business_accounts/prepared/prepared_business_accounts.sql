{{
    config(
        materialized='table',
        unique_key='account_id',
        load_type='full'
    )
}}

with integrated_business_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date_formatted
    from {{ ref('integrated.business_accounts') }}
),

transaction_metrics as (
    select
        account_id,
        count(*) as total_transactions,
        {{ calculate_total_transactions('transaction_amount') }} as total_transaction_volume,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),

business_accounts_prepared as (
    select
        iba.account_id,
        iba.account_name,
        iba.contact_email,
        iba.registration_date_formatted,
        coalesce(tm.total_transactions, 0) as total_transactions,
        coalesce(tm.total_transaction_volume, 0) as total_transaction_volume,
        tm.first_transaction_date,
        tm.last_transaction_date
    from integrated_business_accounts iba
    left join transaction_metrics tm on iba.account_id = tm.account_id
)

select
    account_id,
    account_name,
    contact_email,
    registration_date_formatted,
    total_transactions,
    total_transaction_volume,
    first_transaction_date,
    last_transaction_date,
    {{ set_audit_columns() }}
from business_accounts_prepared;