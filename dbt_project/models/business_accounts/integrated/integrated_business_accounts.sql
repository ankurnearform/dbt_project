{{
    config(
        materialized='view',
        unique_key='account_id'
    )
}}

with deduplicated_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date,
        rank() over (partition by account_id order by registration_date desc) as rank
    from {{ ref('staging.business_accounts') }}
    where contact_email is not null
)

, transaction_totals as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
)

, integrated_accounts as (
    select
        da.account_id,
        da.account_name,
        da.contact_email,
        da.registration_date,
        tt.total_transactions_amount
    from deduplicated_accounts da
    left join transaction_totals tt on da.account_id = tt.account_id
    where da.rank = 1
)

select *
from integrated_accounts
order by registration_date desc;