{{
    config(
        materialized='table',
        unique_key='account_id',
        load_type='incremental'
    )
}}

with transaction_details as (
    select
        account_id,
        count(transaction_id) as total_number_of_transactions,
        {{ calculate_total_transactions('transaction_amount') }} as total_transaction_volume,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),

integrated_accounts as (
    select
        ia.account_id,
        ia.account_name,
        ia.contact_email,
        ia.registration_date,
        coalesce(td.total_number_of_transactions, 0) as total_number_of_transactions,
        coalesce(td.total_transaction_volume, 0) as total_transaction_volume,
        td.first_transaction_date,
        td.last_transaction_date
    from {{ ref('integrated.integrated_accounts') }} ia
    left join transaction_details td
    on ia.account_id = td.account_id
),

prepared_accounts as (
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
    from integrated_accounts
)

select *
from prepared_accounts;