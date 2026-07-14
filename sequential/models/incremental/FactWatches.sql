{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['sk_customerid', 'sk_securityid'],
    merge_update_columns=['sk_dateid_dateremoved'],
) }}
-- FactWatches (watch-list facts). Sequential (3-batch) port.
--   Historical branch (batch 1): silver/FactWatches Historical.sql
--   Incremental branch (batches 2-3): silver/FactWatches Incremental.sql  [Stage 7]
-- One row per (customer, security): dateplaced = first ACTV, dateremoved = last CNCL.
-- SKs resolved as of dateplaced.
{% if is_incremental() %}

-- Incremental merge branch implemented in Stage 7. No-op placeholder until then.
select * from {{ this }} limit 0

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
