```sql
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
        formatted_registration_date
    from {{ ref('integrated_business_accounts') }}
),

transaction_aggregates as (
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
    iba.account_id,
    iba.account_name,
    iba.contact_email,
    iba.formatted_registration_date,
    ta.total_transactions,
    ta.total_transactions_volume,
    ta.first_transaction_date,
    ta.last_transaction_date,
    {{ set_audit_columns() }}
from integrated_business_accounts iba
left join transaction_aggregates ta on iba.account_id = ta.account_id;
```