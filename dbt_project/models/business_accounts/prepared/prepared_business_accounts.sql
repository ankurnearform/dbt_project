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
)

select
    ia.account_id,
    ia.account_name,
    ia.contact_email,
    ia.registration_date,
    coalesce(tm.total_number_of_transactions, 0) as total_number_of_transactions,
    coalesce(tm.total_transaction_volume, 0) as total_transaction_volume,
    tm.first_transaction_date,
    tm.last_transaction_date,
    {{ set_audit_columns() }}
from integrated_accounts ia
left join transaction_metrics tm
    on ia.account_id = tm.account_id
order by ia.registration_date desc;