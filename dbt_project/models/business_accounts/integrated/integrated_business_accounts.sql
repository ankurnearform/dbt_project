with business_accounts_staging as (
    select
        account_id,
        business_name as account_name,
        contact_email,
        to_date(registration_date, 'YYYY-MM-DD') as registration_date
    from {{ source('raw', 'raw_business_accounts') }}
    where contact_email is not null
),

deduplicated_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date,
        row_number() over (partition by account_id order by registration_date desc) as row_num
    from business_accounts_staging
)

select
    da.account_id,
    da.account_name,
    da.contact_email,
    da.registration_date,
    sum(rt.transaction_amount) as total_transactions_amount
from deduplicated_accounts da
join {{ source('raw', 'raw_transactions') }} rt
    on da.account_id = rt.account_id
where da.row_num = 1
group by da.account_id, da.account_name, da.contact_email, da.registration_date
order by da.registration_date desc;