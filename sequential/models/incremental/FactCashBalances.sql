{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}
-- FactCashBalances (running cash balance per account per day). Sequential (3-batch) port.
--   Historical branch (batch 1): gold/FactCashBalances Historical.sql
--   Incremental branch (batches 2-3): gold/FactCashBalances Incremental.sql  [Stage 7]
-- Reads the cumulative running balance from bronzecashtransaction (which carries prior
-- batches' transactions), emitting only this batch's rows, joined to the account version
-- effective that day.
{% if is_incremental() %}

-- Incremental (batch N): append this batch's running balances; join the current account
-- version (per classic). Ported from gold/FactCashBalances Incremental.sql.
{% set b = var('batch') %}
select
  a.sk_customerid,
  a.sk_accountid,
  cast(strftime(c.datevalue, '%Y%m%d') as bigint) as sk_dateid,
  c.cash,
  c.batchid
from {{ ref('bronzecashtransaction') }} c
join {{ ref('DimAccount') }} a on c.accountid = a.accountid and a.iscurrent
where c.batchid = {{ b }}

{% else %}

-- Historical load (batch 1).
select
  a.sk_customerid,
  a.sk_accountid,
  cast(strftime(c.datevalue, '%Y%m%d') as bigint) as sk_dateid,
  c.cash,
  1 as batchid
from {{ ref('bronzecashtransaction') }} c
join {{ ref('DimAccount') }} a
  on c.accountid = a.accountid
 and c.datevalue >= a.effectivedate
 and c.datevalue < a.enddate
where c.batchid = 1

{% endif %}
