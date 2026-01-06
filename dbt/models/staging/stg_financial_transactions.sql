with source as (
  select *
  from {{ source('staging', 'financial_transactions') }}
)
select
  transaction_id,
  account_id,
  transaction_ts,
  posting_date,
  upper(currency) as currency,
  amount::numeric(18,2) as amount,
  merchant_id,
  merchant_name,
  category,
  country,
  city,
  payment_method,
  upper(status) as status,
  is_refund,
  reference
from source


