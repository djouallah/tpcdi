{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}
-- FactHoldings (holding after each trade). Sequential (3-batch) port.
--   Historical branch (batch 1): the FactHoldings INSERT in silver/DimTrade Historical.sql
--     (reads Batch1 HoldingHistory.txt directly; NOT the augmented spark-temp variant).
--   Incremental branch (batches 2-3): augmented_incremental/incremental/FactHoldings
--     Incremental.py  [Stage 7]
-- Each holding row carries the current trade's dimensional keys + close date/time.
{% if is_incremental() %}

-- Incremental append branch implemented in Stage 7. No-op placeholder until then.
select * from {{ this }} limit 0

{% else %}

-- Historical load (batch 1).
select
  h.hh_h_t_id as tradeid,
  h.hh_t_id as currenttradeid,
  dt.sk_customerid,
  dt.sk_accountid,
  dt.sk_securityid,
  dt.sk_companyid,
  dt.sk_closedateid as sk_dateid,
  dt.sk_closetimeid as sk_timeid,
  dt.tradeprice as currentprice,
  h.hh_after_qty as currentholding,
  1 as batchid
from {{ read_pipe('Batch1/HoldingHistory.txt',
  "{'hh_h_t_id': 'BIGINT', 'hh_t_id': 'BIGINT', 'hh_before_qty': 'INTEGER', 'hh_after_qty': 'INTEGER'}") }} h
join {{ ref('DimTrade') }} dt on dt.tradeid = h.hh_t_id

{% endif %}
