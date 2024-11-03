
  
    
        create or replace table `hive_metastore`.`default`.`prepared_business_accounts`
      
      using delta
      
      
      
      
      
      
      
      as
      

with integrated_accounts as (
    select
        account_id,
        account_name,
        contact_email,
        registration_date
    from `hive_metastore`.`default`.`integrated_business_accounts`
),

transaction_metrics as (
    select
        account_id,
        count(transaction_id) as total_number_of_transactions,
        
    sum(transaction_amount)
 as total_transaction_volume,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from `hive_metastore`.`raw`.`raw_transactions`
    group by account_id
),

account_aggregated_data as (
    select
        ia.account_id,
        ia.account_name,
        ia.contact_email,
        ia.registration_date,
        coalesce(tm.total_number_of_transactions, 0) as total_number_of_transactions,
        coalesce(tm.total_transaction_volume, 0) as total_transaction_volume,
        tm.first_transaction_date,
        tm.last_transaction_date
    from integrated_accounts ia
    left join transaction_metrics tm on ia.account_id = tm.account_id
)

select
    account_id,
    account_name,
    contact_email,
    registration_date,
    total_number_of_transactions,
    total_transaction_volume,
    first_transaction_date,
    last_transaction_date,
    
    CAST(current_timestamp() AS TIMESTAMP) as created_at,
    CAST(current_timestamp() AS TIMESTAMP) as updated_at,
    CAST(current_timestamp() AS TIMESTAMP) as processed_at

from account_aggregated_data;
  