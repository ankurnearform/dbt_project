{{
    config(
        materialized='view',
        unique_key='account_id'
    )
}}

with deduplicated_business_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date
    from (
        select
            account_id,
            account_name,
            contact_email,
            registration_date,
            row_number() over (partition by account_id order by registration_date desc) as rn
        from {{ ref('staging.business_accounts') }}
    ) as ranked_accounts
    where rn = 1
),

transaction_totals as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from {{ source('raw', 'raw_transactions') }}
    group by account_id
)

select
    d.account_id,
    d.account_name,
    d.contact_email,
    d.registration_date,
    coalesce(t.total_transactions_amount, 0) as total_transactions_amount
from deduplicated_business_accounts d
left join transaction_totals t
    on d.account_id = t.account_id
order by d.registration_date desc;