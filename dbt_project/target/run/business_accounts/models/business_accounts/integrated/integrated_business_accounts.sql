create or replace view `hive_metastore`.`default`.`integrated_business_accounts`
  
  
  
  as
    

with deduplicated_accounts as (
    select
        account_id,
        max(account_name) as account_name,
        max(contact_email) as contact_email,
        max(registration_date) as registration_date
    from (
        select
            account_id,
            account_name,
            contact_email,
            registration_date
        from `hive_metastore`.`default`.`staging_business_accounts`
    ) as accounts
    group by account_id
),

transactions_sum as (
    select
        account_id,
        sum(transaction_amount) as total_transactions_amount
    from `hive_metastore`.`raw`.`raw_transactions`
    group by account_id
)

select
    a.account_id,
    a.account_name,
    a.contact_email,
    a.registration_date,
    coalesce(t.total_transactions_amount, 0) as total_transactions_amount
from deduplicated_accounts a
left join transactions_sum t
    on a.account_id = t.account_id
order by a.registration_date desc;
