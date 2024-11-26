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
        account_name,
        contact_email,
        registration_date
    from {{ ref('integrated.business_accounts') }}
),

transactions_aggregated as (
    select
        rt.account_id,
        count(*) as total_number_of_transactions,
        {{ calculate_total_transactions('rt.transaction_amount') }} as total_transactions_volume,
        min(rt.transaction_date) as first_transaction_date,
        max(rt.transaction_date) as last_transaction_date
    from {{ source('raw', 'raw_transactions') }} rt
    group by rt.account_id
),

accounts_with_transactions as (
    select
        ba.account_id,
        ba.account_name,
        ba.contact_email,
        ba.registration_date,
        ta.total_number_of_transactions,
        ta.total_transactions_volume,
        ta.first_transaction_date,
        ta.last_transaction_date
    from business_accounts ba
    join transactions_aggregated ta
        on ba.account_id = ta.account_id
)

select
    account_id,
    account_name,
    contact_email,
    registration_date,
    total_number_of_transactions,
    total_transactions_volume,
    first_transaction_date,
    last_transaction_date,
    {{ set_audit_columns() }}
from accounts_with_transactions;