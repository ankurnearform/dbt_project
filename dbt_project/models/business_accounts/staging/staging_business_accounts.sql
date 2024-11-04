{{
    config(
        materialized='view',
        unique_key='account_id'
    )
}}

with business_accounts as (
    select
        account_id,
        business_name as account_name,
        contact_email,
        to_date(registration_date, 'YYYY-MM-DD') as registration_date_formatted
    from {{ source('raw', 'raw_business_accounts') }}
    where contact_email is not null
)

select *
from business_accounts;