with tx as (
  select *
  from {{ ref('stg_financial_transactions') }}
),
accounts as (
  select *
  from {{ ref('dim_accounts') }}
),
merchants as (
  select *
  from {{ ref('dim_merchants') }}
)
select
  tx.transaction_id,
  tx.account_id,
  tx.merchant_id,
  tx.transaction_ts,
  tx.posting_date,
  tx.currency,
  tx.amount,
  tx.status,
  tx.is_refund,
  tx.category,
  tx.country,
  tx.city,
  tx.payment_method,
  tx.reference,
  accounts.first_seen_ts as account_first_seen_ts,
  merchants.merchant_name as merchant_name
from tx
left join accounts
  on tx.account_id = accounts.account_id
left join merchants
  on tx.merchant_id = merchants.merchant_id


