{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='tradeid',
    merge_update_columns=['sk_closedateid', 'sk_closetimeid', 'status', 'type', 'cashflag',
      'quantity', 'bidprice', 'executedby', 'tradeprice', 'fee', 'commission', 'tax', 'batchid'],
    merge_update_condition='DBT_INTERNAL_DEST.sk_closedateid is null',
) }}
-- DimTrade (one row per trade). Sequential (3-batch) port.
--   Historical branch (batch 1): silver/DimTrade Historical.sql
--   Incremental branch (batches 2-3): silver/DimTrade Incremental.sql
-- cdc 'I' inserts a new trade; cdc 'U' updates the OPEN trade in place (status, close
-- date/time when it hits CMPT/CNCL, and price/fee/commission/tax). Unlike the single-pass
-- model this DOES overwrite Batch1 prices — spec-correct. merge_update_condition restricts
-- the update to still-open trades (sk_closedateid is null), matching the classic MERGE's
-- ON ... AND t.sk_closedateid is null. Create keys are never updated (kept from insert).
{% if is_incremental() %}

-- Incremental (batch N): this batch's Trade.txt CDC rows.
{% set b = var('batch') %}
with traderaw as (
  select
    tradeid, t_dts,
    case when cdc_flag = 'I' then t_dts end as create_ts,
    case when status in ('CMPT', 'CNCL') then t_dts end as close_ts,
    {{ status_longform('status') }} as status,
    case t_tt_id
      when 'TMB' then 'Market Buy' when 'TMS' then 'Market Sell' when 'TSL' then 'Stop Loss'
      when 'TLS' then 'Limit Sell' when 'TLB' then 'Limit Buy' end as type,
    (cashflag = 1) as cashflag,
    t_s_symb, quantity, bidprice, t_ca_id, executedby, tradeprice, fee, commission, tax
  from {{ read_pipe('Batch' ~ b ~ '/Trade.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'tradeid': 'BIGINT', 't_dts': 'TIMESTAMP',
      'status': 'VARCHAR', 't_tt_id': 'VARCHAR', 'cashflag': 'TINYINT', 't_s_symb': 'VARCHAR',
      'quantity': 'INTEGER', 'bidprice': 'DOUBLE', 't_ca_id': 'BIGINT', 'executedby': 'VARCHAR',
      'tradeprice': 'DOUBLE', 'fee': 'DOUBLE', 'commission': 'DOUBLE', 'tax': 'DOUBLE'}") }}
),
trades as (
  -- latest record per trade this batch drives the update; min(create_ts) is the 'I' time.
  select
    tradeid,
    min(create_ts) as create_ts,
    max_by({'close_ts': close_ts, 'status': status, 'type': type, 'cashflag': cashflag,
      't_s_symb': t_s_symb, 'quantity': quantity, 'bidprice': bidprice, 't_ca_id': t_ca_id,
      'executedby': executedby, 'tradeprice': tradeprice, 'fee': fee, 'commission': commission,
      'tax': tax}, t_dts) as cr
  from traderaw
  group by tradeid
)
select
  t.tradeid,
  da.sk_brokerid,
  {{ sk_dateid('t.create_ts') }} as sk_createdateid,
  {{ sk_timeid('t.create_ts') }} as sk_createtimeid,
  {{ sk_dateid('t.cr.close_ts') }} as sk_closedateid,
  {{ sk_timeid('t.cr.close_ts') }} as sk_closetimeid,
  t.cr.status as status,
  t.cr.type as type,
  t.cr.cashflag as cashflag,
  ds.sk_securityid,
  ds.sk_companyid,
  t.cr.quantity as quantity,
  t.cr.bidprice as bidprice,
  da.sk_customerid,
  da.sk_accountid,
  t.cr.executedby as executedby,
  t.cr.tradeprice as tradeprice,
  t.cr.fee as fee,
  t.cr.commission as commission,
  t.cr.tax as tax,
  {{ b }} as batchid
from trades t
join {{ ref('DimSecurity') }} ds on ds.symbol = t.cr.t_s_symb and ds.iscurrent
join {{ ref('DimAccount') }} da on t.cr.t_ca_id = da.accountid and da.iscurrent

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
