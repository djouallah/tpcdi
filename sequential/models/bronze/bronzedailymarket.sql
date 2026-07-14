{{ config(materialized='table') }}
-- Sequential bronze: per-batch cumulative daily market with the trailing 52-week high/low
-- precomputed. Ported from shannon-barrow/databricks-tpc-di
--   src/incremental_batches/bronze/ingest_dailymarketincremental.sql
-- Rebuilt each batch over Batch1..N so the 52-week window (364 trailing rows per symbol,
-- ordered by date) is correct across batch boundaries. FactMarketHistory reads batchid=N.
-- Batch1 DailyMarket.txt is 6-col; Batch2/3 carry a cdc_flag/cdc_dsn prefix (8-col).
-- Carries the 52-week fix: min_by/max_by keyed on price, WINDOW ordered by dm_date.
with dailymarket as (
  select dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, 1 as batchid
  from {{ read_pipe('Batch1/DailyMarket.txt',
    "{'dm_date': 'DATE', 'dm_s_symb': 'VARCHAR', 'dm_close': 'DOUBLE',
      'dm_high': 'DOUBLE', 'dm_low': 'DOUBLE', 'dm_vol': 'INTEGER'}") }}
{% if var('batch') | int >= 2 %}
  union all
  select dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol,
    {{ batchid_from_filename() }} as batchid
  from {{ read_pipe('Batch[2-' ~ var('batch') ~ ']/DailyMarket.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'dm_date': 'DATE',
      'dm_s_symb': 'VARCHAR', 'dm_close': 'DOUBLE', 'dm_high': 'DOUBLE',
      'dm_low': 'DOUBLE', 'dm_vol': 'INTEGER'}", with_filename=true) }}
{% endif %}
),
markethistory as (
  select
    dm.*,
    min_by({'dm_low': dm_low, 'dm_date': dm_date}, dm_low) over (
      partition by dm_s_symb order by dm_date asc
      rows between 364 preceding and current row
    ) as fiftytwoweeklow,
    max_by({'dm_high': dm_high, 'dm_date': dm_date}, dm_high) over (
      partition by dm_s_symb order by dm_date asc
      rows between 364 preceding and current row
    ) as fiftytwoweekhigh
  from dailymarket dm
)
select
  dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, batchid,
  fiftytwoweekhigh.dm_high as fiftytwoweekhigh,
  cast(strftime(fiftytwoweekhigh.dm_date, '%Y%m%d') as bigint) as sk_fiftytwoweekhighdate,
  fiftytwoweeklow.dm_low as fiftytwoweeklow,
  cast(strftime(fiftytwoweeklow.dm_date, '%Y%m%d') as bigint) as sk_fiftytwoweeklowdate
from markethistory
