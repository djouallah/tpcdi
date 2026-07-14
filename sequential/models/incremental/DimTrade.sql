{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='tradeid',
) }}
-- DimTrade (one row per trade). Sequential (3-batch) port.
--   Historical branch (batch 1): silver/DimTrade Historical.sql
--   Incremental branch (batches 2-3): silver/DimTrade Incremental.sql  [Stage 6]
-- Batch1 gives the full trade + trade-history load. Create timestamp = earliest history
-- event; close timestamp = the CMPT/CNCL event. Dimensional SKs are attached as of the
-- create date.
{% if is_incremental() %}

-- Incremental merge branch implemented in Stage 6. No-op placeholder until then.
select * from {{ this }} limit 0

{% else %}

-- Historical load (batch 1).
with tradehistory as (
  select
    tradeid,
    min(th_dts) as create_ts,
    max_by({'th_dts': th_dts, 'status': status}, th_dts) as current_status
  from {{ read_pipe('Batch1/TradeHistory.txt',
    "{'tradeid': 'BIGINT', 'th_dts': 'TIMESTAMP', 'status': 'VARCHAR'}") }}
  group by tradeid
),
trades as (
  select
    t.t_id as tradeid,
    ct.create_ts,
    case when ct.current_status.status in ('CMPT', 'CNCL') then ct.current_status.th_dts end as close_ts,
    ct.current_status.status as status,
    (t.t_is_cash = 1) as cashflag,
    t.t_tt_id, t.t_s_symb, t.quantity, t.bidprice, t.t_ca_id, t.executedby,
    t.tradeprice, t.fee, t.commission, t.tax
  from {{ read_pipe('Batch1/Trade.txt',
    "{'t_id': 'BIGINT', 't_dts': 'TIMESTAMP', 't_st_id': 'VARCHAR', 't_tt_id': 'VARCHAR',
      't_is_cash': 'TINYINT', 't_s_symb': 'VARCHAR', 'quantity': 'INTEGER', 'bidprice': 'DOUBLE',
      't_ca_id': 'BIGINT', 'executedby': 'VARCHAR', 'tradeprice': 'DOUBLE', 'fee': 'DOUBLE',
      'commission': 'DOUBLE', 'tax': 'DOUBLE'}") }} t
  join tradehistory ct on t.t_id = ct.tradeid
)
select
  trade.tradeid,
  da.sk_brokerid,
  {{ sk_dateid('create_ts') }} as sk_createdateid,
  {{ sk_timeid('create_ts') }} as sk_createtimeid,
  {{ sk_dateid('close_ts') }} as sk_closedateid,
  {{ sk_timeid('close_ts') }} as sk_closetimeid,
  {{ status_longform('trade.status') }} as status,
  case trade.t_tt_id
    when 'TMB' then 'Market Buy'
    when 'TMS' then 'Market Sell'
    when 'TSL' then 'Stop Loss'
    when 'TLS' then 'Limit Sell'
    when 'TLB' then 'Limit Buy'
  end as type,
  trade.cashflag,
  ds.sk_securityid,
  ds.sk_companyid,
  trade.quantity,
  trade.bidprice,
  da.sk_customerid,
  da.sk_accountid,
  trade.executedby,
  trade.tradeprice,
  trade.fee,
  trade.commission,
  trade.tax,
  1 as batchid
from trades trade
join {{ ref('DimSecurity') }} ds
  on ds.symbol = trade.t_s_symb
 and date(trade.create_ts) >= ds.effectivedate
 and date(trade.create_ts) < ds.enddate
join {{ ref('DimAccount') }} da
  on trade.t_ca_id = da.accountid
 and date(trade.create_ts) >= da.effectivedate
 and date(trade.create_ts) < da.enddate

{% endif %}
