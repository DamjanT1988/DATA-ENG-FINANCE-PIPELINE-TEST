select
  account_id,
  max(country) as country,
  max(city) as city,
  min(transaction_ts) as first_seen_ts
from {{ ref('stg_financial_transactions') }}
group by 1


