{{
    config(
        materialized='table',
        unique_key='account_id',
        load_type='incremental'
    )
}}

with integrated_business_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date
    from {{ ref('integrated.integrated_business_accounts') }}
),

transaction_metrics as (
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
        a.account_id,
        a.account_name,
        a.contact_email,
        a.registration_date,
        coalesce(t.total_number_of_transactions, 0) as total_number_of_transactions,
        coalesce(t.total_transaction_volume, 0) as total_transaction_volume,
        t.first_transaction_date,
        t.last_transaction_date
    from integrated_business_accounts a
    left join transaction_metrics t on a.account_id = t.account_id
)

select
    {{ set_audit_columns() }},
    p.account_id,
    p.account_name,
    p.contact_email,
    p.registration_date,
    p.total_number_of_transactions,
    p.total_transaction_volume,
    p.first_transaction_date,
    p.last_transaction_date
from prepared_business_accounts p;