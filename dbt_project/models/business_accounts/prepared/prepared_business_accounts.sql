{{
    config(
        materialized='table',
        unique_key='account_id',
        load_type='full'
    )
}}

with integrated_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date,
        total_transactions_amount
    from {{ ref('integrated.integrated_accounts') }}
),

transaction_details as (
    select
        t.account_id,
        count(*) as total_number_of_transactions,
        {{ calculate_total_transactions('transaction_amount') }} as total_transaction_volume,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from {{ source('raw', 'raw_transactions') }} t
    join integrated_accounts a on t.account_id = a.account_id
    group by t.account_id
),

business_accounts_prepared as (
    select
        a.account_id,
        a.account_name,
        a.contact_email,
        a.registration_date,
        coalesce(d.total_number_of_transactions, 0) as total_number_of_transactions,
        coalesce(d.total_transaction_volume, 0) as total_transaction_volume,
        d.first_transaction_date,
        d.last_transaction_date
    from integrated_accounts a
    left join transaction_details d on a.account_id = d.account_id
),

business_accounts_with_audit as (
    select
        *,
        {{ set_audit_columns() }}
    from business_accounts_prepared
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
    created_at,
    updated_at,
    processed_at
from business_accounts_with_audit;