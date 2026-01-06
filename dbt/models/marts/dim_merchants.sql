select
  merchant_id,
  max(merchant_name) as merchant_name,
  max(category) as category
from {{ ref('stg_financial_transactions') }}
where merchant_id is not null
group by 1


