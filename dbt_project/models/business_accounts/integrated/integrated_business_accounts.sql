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
)

select
    da.account_id,
    da.account_name,
    da.contact_email,
    da.formatted_registration_date,
    coalesce(sum(t.transaction_amount), 0) as total_transactions_amount
from deduplicated_accounts da
left join {{ source('raw', 'raw_transactions') }} t on da.account_id = t.account_id
where da.rn = 1
group by da.account_id, da.account_name, da.contact_email, da.formatted_registration_date
order by da.formatted_registration_date desc;