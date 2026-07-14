{{ config(materialized='table') }}
-- Sequential bronze: per-batch cumulative Prospect staging.
-- Ported from shannon-barrow/databricks-tpc-di
--   src/incremental_batches/bronze/ingest_prospectincremental.sql
-- Prospect.csv is a full cumulative snapshot per batch. Rebuilt each batch over Batch1..N
-- (the `Batch[1-N]` glob = batches seen so far), collapsing identical rows
-- (recordbatchid = latest batch a row appears in, batchid = first) and deriving the
-- marketingnameplate tags. DimCustomer (batchid=1) and Prospect (as-of batch) read this.
with prospect_raw as (
  select
    agencyid, lastname, firstname, middleinitial, gender,
    addressline1, addressline2, postalcode, city, state, country, phone,
    income, numbercars, numberchildren, maritalstatus, age, creditrating,
    ownorrentflag, employer, numbercreditcards, networth,
    {{ batchid_from_filename() }} as batchid
  from {{ read_csvfile('Batch[1-' ~ var('batch') ~ ']/Prospect.csv',
    "{'agencyid': 'VARCHAR', 'lastname': 'VARCHAR', 'firstname': 'VARCHAR',
      'middleinitial': 'VARCHAR', 'gender': 'VARCHAR', 'addressline1': 'VARCHAR',
      'addressline2': 'VARCHAR', 'postalcode': 'VARCHAR', 'city': 'VARCHAR',
      'state': 'VARCHAR', 'country': 'VARCHAR', 'phone': 'VARCHAR',
      'income': 'VARCHAR', 'numbercars': 'INTEGER', 'numberchildren': 'INTEGER',
      'maritalstatus': 'VARCHAR', 'age': 'INTEGER', 'creditrating': 'INTEGER',
      'ownorrentflag': 'VARCHAR', 'employer': 'VARCHAR',
      'numbercreditcards': 'INTEGER', 'networth': 'BIGINT'}", with_filename=true) }}
),
tagged as (
  select
    * exclude (batchid),
    batchid,
    nullif(concat_ws('+',
      case when networth > 1000000 or try_cast(income as double) > 200000 then 'HighValue' end,
      case when numberchildren > 3 or numbercreditcards > 5 then 'Expenses' end,
      case when age > 45 then 'Boomer' end,
      case when try_cast(income as double) < 50000 or creditrating < 600 or networth < 100000 then 'MoneyAlert' end,
      case when numbercars > 3 or numbercreditcards > 7 then 'Spender' end,
      case when age < 25 and networth > 1000000 then 'Inherited' end
    ), '') as marketingnameplate
  from prospect_raw
)
select
  * exclude (batchid),
  max(batchid) as recordbatchid,
  min(batchid) as batchid
from tagged
group by all
