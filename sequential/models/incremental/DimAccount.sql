{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sk_accountid',
    merge_update_columns=['iscurrent', 'enddate'],
) }}
-- DimAccount, SCD2. Sequential (3-batch) port.
--   Historical branch (batch 1): silver/DimAccount Historical.sql
--   Incremental branch (batches 2-3): silver/DimAccount Incremental.sql +
--     augmented_incremental/bronze/account_updates_from_customer.py  [Stage 9]
-- Each account version is intersected with the owning customer's version window so the
-- account and customer surrogate keys stay time-consistent.
{% if is_incremental() %}

-- Incremental merge branch implemented in Stage 9 (the hardest model). No-op placeholder.
select * from {{ this }} limit 0

{% else %}

-- Historical load (batch 1): account versions come only from CustomerMgmt.xml.
{% set ff = "over (partition by accountid order by update_ts rows between unbounded preceding and current row)" %}
with account as (
  select
    accountid, customerid,
    coalesce(accountdesc, last_value(accountdesc ignore nulls) {{ ff }}) as accountdesc,
    coalesce(taxstatus,   last_value(taxstatus   ignore nulls) {{ ff }}) as taxstatus,
    coalesce(brokerid,    last_value(brokerid    ignore nulls) {{ ff }}) as brokerid,
    coalesce(status,      last_value(status      ignore nulls) {{ ff }}) as status,
    date(update_ts) as effectivedate,
    coalesce(
      lead(date(update_ts)) over (partition by accountid order by update_ts),
      date '9999-12-31') as enddate,
    1 as batchid
  from {{ ref('stg_customermgmt') }}
  where actiontype not in ('UPDCUST', 'INACT')
),
with_cust_updates as (
  select
    a.accountid, a.accountdesc, a.taxstatus, a.brokerid, a.status, a.batchid,
    c.sk_customerid,
    case when a.effectivedate < c.effectivedate then c.effectivedate else a.effectivedate end as effectivedate,
    case when a.enddate > c.enddate then c.enddate else a.enddate end as enddate
  from account a
  full outer join {{ ref('DimCustomer') }} c
    on a.batchid = c.batchid
   and a.customerid = c.customerid
   and c.enddate > a.effectivedate
   and c.effectivedate < a.enddate
  where a.effectivedate < a.enddate
)
select
  cast(concat(strftime(a.effectivedate, '%Y%m%d'), cast(a.accountid as varchar)) as bigint) as sk_accountid,
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.taxstatus,
  a.status,
  (a.enddate = date '9999-12-31') as iscurrent,
  a.batchid,
  a.effectivedate,
  a.enddate
from with_cust_updates a
join {{ ref('DimBroker') }} b on a.brokerid = b.brokerid

{% endif %}
