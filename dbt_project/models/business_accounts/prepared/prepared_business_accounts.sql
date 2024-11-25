{{
    config(
        materialized='table',
        unique_key='account_id',
        load_type='incremental'
    )
}}

with raw_transactions_aggregated as (
    select
        account_id,
        count(*) as total_number_of_transactions,
        {{ calculate_total_transactions('transaction_amount') }} as total_transaction_volume,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
),

account_metrics as (
    select
        ia.account_id,
        ia.account_name,
        ia.contact_email,
        ia.registration_date,
        rta.total_number_of_transactions,
        rta.total_transaction_volume,
        rta.first_transaction_date,
        rta.last_transaction_date
    from {{ ref('integrated.integrated_accounts') }} ia
    left join raw_transactions_aggregated rta
    on ia.account_id = rta.account_id
),

final_output as (
    select
        am.account_id,
        am.account_name,
        am.contact_email,
        am.registration_date,
        am.total_number_of_transactions,
        am.total_transaction_volume,
        am.first_transaction_date,
        am.last_transaction_date,
        {{ set_audit_columns() }}
    from account_metrics am
)

select *
from final_output;