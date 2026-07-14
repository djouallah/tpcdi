{{ config(materialized='table') }}
-- Sequential bronze: per-batch cumulative cash balance per account per day.
-- Ported from shannon-barrow/databricks-tpc-di
--   src/incremental_batches/gold/FactCashBalances Historical.sql (the
--   CashTransactionIncremental CTE).
-- Rebuilt each batch over Batch1..N so the running balance
-- (sum over account ordered by date) correctly carries prior batches' transactions.
-- FactCashBalances reads batchid=N. Batch1 CashTransaction.txt is 4-col; Batch2/3 add a
-- cdc_flag/cdc_dsn prefix (6-col).
with alltransactions as (
  select accountid, date(ct_dts) as datevalue, sum(ct_amt) as account_daily_total, 1 as batchid
  from {{ read_pipe('Batch1/CashTransaction.txt',
    "{'accountid': 'BIGINT', 'ct_dts': 'TIMESTAMP', 'ct_amt': 'DOUBLE', 'ct_name': 'VARCHAR'}") }}
  group by all
{% if var('batch') | int >= 2 %}
  union all
  select accountid, to_date(ct_dts) as datevalue, sum(ct_amt) as account_daily_total,
    {{ batchid_from_filename() }} as batchid
  from {{ read_pipe('Batch[2-' ~ var('batch') ~ ']/CashTransaction.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'accountid': 'BIGINT',
      'ct_dts': 'TIMESTAMP', 'ct_amt': 'DOUBLE', 'ct_name': 'VARCHAR'}", with_filename=true) }}
  group by all
{% endif %}
)
select
  accountid,
  datevalue,
  sum(account_daily_total) over (partition by accountid order by datevalue) as cash,
  batchid
from alltransactions
