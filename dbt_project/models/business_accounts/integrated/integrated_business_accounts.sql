{{ 
    config(
        materialized='view',
        unique_key='account_id'
    )
}}

with business_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        formatted_registration_date
    from {{ ref('staging_business_accounts') }}
),

deduplicated_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        formatted_registration_date,
        row_number() over (partition by account_id order by formatted_registration_date desc) as rn
    from business_accounts
),

transactions_aggregated as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
)

select
    da.account_id,
    da.account_name,
    da.contact_email,
    da.formatted_registration_date,
    coalesce(ta.total_transactions_amount, 0) as total_transactions_amount
from deduplicated_accounts da
left join transactions_aggregated ta on da.account_id = ta.account_id
where da.rn = 1
order by da.formatted_registration_date desc;