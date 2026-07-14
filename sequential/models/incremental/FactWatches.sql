{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['sk_customerid', 'sk_securityid'],
    merge_update_columns=['sk_dateid_dateremoved', 'batchid'],
    merge_update_condition='DBT_INTERNAL_DEST.sk_dateid_dateremoved is null',
) }}
-- FactWatches (watch-list facts). Sequential (3-batch) port.
--   Historical branch (batch 1): silver/FactWatches Historical.sql
--   Incremental branch (batches 2-3): silver/FactWatches Incremental.sql
-- One row per (customer, security). ACTV rows insert a new watch; CNCL rows close the
-- existing OPEN watch by setting sk_dateid_dateremoved (merge_update_condition restricts
-- the update to still-open watches). FactWatches has no natural keys — CNCL rows resolve
-- the customer/security back through the current dims to get the surrogate keys to match.
{% if is_incremental() %}

-- Incremental (batch N).
{% set b = var('batch') %}
with watches as (
  select
    w_c_id as customerid,
    w_s_symb as symbol,
    date(min(case when w_action != 'CNCL' then w_dts end)) as dateplaced,
    date(max(case when w_action = 'CNCL' then w_dts end)) as dateremoved
  from {{ read_pipe('Batch' ~ b ~ '/WatchHistory.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'w_c_id': 'BIGINT', 'w_s_symb': 'VARCHAR',
      'w_dts': 'TIMESTAMP', 'w_action': 'VARCHAR'}") }}
  group by w_c_id, w_s_symb
),
-- New watches placed this batch -> INSERT.
watch_actv as (
  select
    c.sk_customerid, s.sk_securityid,
    cast(strftime(wh.dateplaced, '%Y%m%d') as bigint) as sk_dateid_dateplaced,
    cast(strftime(wh.dateremoved, '%Y%m%d') as bigint) as sk_dateid_dateremoved,
    {{ b }} as batchid
  from watches wh
  join {{ ref('DimSecurity') }} s on s.symbol = wh.symbol and s.iscurrent
  join {{ ref('DimCustomer') }} c on wh.customerid = c.customerid and c.iscurrent
  where wh.dateplaced is not null
    and c.sk_customerid is not null and s.sk_securityid is not null
),
-- Cancellations this batch -> resolve the existing OPEN watch's SKs -> UPDATE dateremoved.
watch_cncl as (
  select
    fw.sk_customerid, fw.sk_securityid,
    cast(null as bigint) as sk_dateid_dateplaced,
    cast(strftime(w.dateremoved, '%Y%m%d') as bigint) as sk_dateid_dateremoved,
    {{ b }} as batchid
  from (select sk_customerid, sk_securityid from {{ this }} where sk_dateid_dateremoved is null) fw
  join {{ ref('DimCustomer') }} c on fw.sk_customerid = c.sk_customerid
  join {{ ref('DimSecurity') }} s on fw.sk_securityid = s.sk_securityid
  join (select customerid, symbol, dateremoved from watches where dateplaced is null) w
    on w.customerid = c.customerid and w.symbol = s.symbol
)
select * from watch_actv
union all
select * from watch_cncl

{% else %}

-- Historical load (batch 1).
with watches as (
  select
    w_c_id as customerid,
    w_s_symb as symbol,
    date(min(w_dts)) as dateplaced,
    date(max(case when w_action = 'CNCL' then w_dts end)) as dateremoved
  from {{ read_pipe('Batch1/WatchHistory.txt',
    "{'w_c_id': 'BIGINT', 'w_s_symb': 'VARCHAR', 'w_dts': 'TIMESTAMP', 'w_action': 'VARCHAR'}") }}
  group by w_c_id, w_s_symb
)
select
  c.sk_customerid,
  s.sk_securityid,
  cast(strftime(wh.dateplaced, '%Y%m%d') as bigint) as sk_dateid_dateplaced,
  cast(strftime(wh.dateremoved, '%Y%m%d') as bigint) as sk_dateid_dateremoved,
  1 as batchid
from watches wh
join {{ ref('DimSecurity') }} s
  on s.symbol = wh.symbol
 and wh.dateplaced >= s.effectivedate
 and wh.dateplaced < s.enddate
join {{ ref('DimCustomer') }} c
  on wh.customerid = c.customerid
 and wh.dateplaced >= c.effectivedate
 and wh.dateplaced < c.enddate
where c.sk_customerid is not null
  and s.sk_securityid is not null

{% endif %}
