{
    config(
        materialized='view',
        unique_key='account_id'
    )
}

with deduplicated_business_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        formatted_registration_date,
        row_number() over (partition by account_id order by formatted_registration_date desc) as rn
    from {{ ref('staging_business_accounts') }}
)

, transaction_totals as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
)

select
    dba.account_id,
    dba.account_name,
    dba.contact_email,
    dba.formatted_registration_date,
    coalesce(tt.total_transactions_amount, 0) as total_transactions_amount
from deduplicated_business_accounts dba
left join transaction_totals tt on dba.account_id = tt.account_id
where dba.rn = 1
order by dba.formatted_registration_date desc;