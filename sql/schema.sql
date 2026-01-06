-- Warehouse schemas
create schema if not exists raw;
create schema if not exists staging;
create schema if not exists analytics;

-- Raw append-only landing table (keeps original strings, adds ingestion timestamp)
create table if not exists raw.financial_transactions_raw (
  transaction_id text not null,
  account_id text not null,
  transaction_ts text,
  posting_date text,
  currency text,
  amount text,
  merchant_id text,
  merchant_name text,
  category text,
  country text,
  city text,
  payment_method text,
  status text,
  is_refund text,
  reference text,
  ingestion_ts timestamptz not null default now()
);

create index if not exists idx_fin_txn_raw_transaction_id
  on raw.financial_transactions_raw (transaction_id);

-- Cleaned + deduped staging table (typed)
create table if not exists staging.financial_transactions (
  transaction_id text primary key,
  account_id text not null,
  transaction_ts timestamptz not null,
  posting_date date not null,
  currency text not null,
  amount numeric(18,2) not null,
  merchant_id text,
  merchant_name text,
  category text not null,
  country text,
  city text,
  payment_method text,
  status text not null,
  is_refund boolean not null,
  reference text
);

create index if not exists idx_fin_txn_staging_account_id
  on staging.financial_transactions (account_id);

create index if not exists idx_fin_txn_staging_posting_date
  on staging.financial_transactions (posting_date);


