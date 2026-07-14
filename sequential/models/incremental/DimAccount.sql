{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sk_accountid',
    merge_update_columns=['iscurrent', 'enddate'],
) }}
-- depends_on: {{ ref('BatchDate') }}
-- (BatchDate is ref'd only inside the is_incremental() branch, so dbt can't infer the
--  dep at parse time — is_incremental() is False during parsing. This hint declares it.)
-- DimAccount, SCD2. Sequential (3-batch) port.
--   Historical branch (batch 1): silver/DimAccount Historical.sql
--   Incremental branch (batches 2-3): silver/DimAccount Incremental.sql (the customer-
--     driven update is the cust_updates CTE — done inline in SQL, so the streaming
--     account_updates_from_customer.py companion is not needed).
-- Two update sources: (a) Account.txt CDC rows this batch; (b) accounts whose owning
-- customer got a new version this batch (they get a new version with the new sk_customerid).
-- Same SCD2 close+insert shape as DimCustomer: new_rows (new sk) INSERT; close_rows
-- (existing sk) set iscurrent=false, enddate. Disjoint by sk_accountid.
{% if is_incremental() %}

-- Incremental (batch N).
{% set b = var('batch') %}
with acct_cdc as (
  -- (a) this batch's Account.txt CDC -> new versions, current customer + broker.
  select
    a.accountid, brk.sk_brokerid, dc.sk_customerid,
    a.accountdesc, a.taxstatus,
    {{ status_longform('a.status') }} as status,
    (select batchdate from {{ ref('BatchDate') }} where batchid = {{ b }}) as effectivedate,
    date '9999-12-31' as enddate
  from {{ read_pipe('Batch' ~ b ~ '/Account.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'accountid': 'BIGINT', 'brokerid': 'BIGINT',
      'customerid': 'BIGINT', 'accountdesc': 'VARCHAR', 'taxstatus': 'TINYINT', 'status': 'VARCHAR'}") }} a
  join {{ ref('DimCustomer') }} dc on dc.iscurrent and dc.customerid = a.customerid
  join {{ ref('DimBroker') }} brk on a.brokerid = brk.brokerid
),
cust_updates as (
  -- (b) accounts whose owning customer got a new version this batch -> new sk_customerid.
  select
    a.accountid, a.sk_brokerid, ci.sk_customerid,
    a.accountdesc, a.taxstatus, a.status,
    ci.effectivedate, ci.enddate
  from (select sk_customerid, customerid, effectivedate, enddate
        from {{ ref('DimCustomer') }} where iscurrent and batchid = {{ b }}) ci
  join (select sk_customerid, customerid, enddate
        from {{ ref('DimCustomer') }} where not iscurrent and batchid < {{ b }}) ch
    on ci.customerid = ch.customerid and ch.enddate = ci.effectivedate
  join {{ this }} a on ch.sk_customerid = a.sk_customerid and a.iscurrent
),
all_updates as (
  select
    coalesce(a.accountid, b2.accountid) as accountid,
    coalesce(a.sk_brokerid, b2.sk_brokerid) as sk_brokerid,
    coalesce(a.sk_customerid, b2.sk_customerid) as sk_customerid,
    coalesce(a.accountdesc, b2.accountdesc) as accountdesc,
    coalesce(a.taxstatus, b2.taxstatus) as taxstatus,
    coalesce(a.status, b2.status) as status,
    coalesce(a.effectivedate, b2.effectivedate) as effectivedate,
    coalesce(a.enddate, b2.enddate) as enddate
  from acct_cdc a
  full outer join cust_updates b2 on a.accountid = b2.accountid
),
new_rows as (
  select
    cast(concat(strftime(effectivedate, '%Y%m%d'), cast(accountid as varchar)) as bigint) as sk_accountid,
    accountid, sk_brokerid, sk_customerid, accountdesc, taxstatus, status,
    true as iscurrent, {{ b }} as batchid, effectivedate, enddate
  from all_updates
),
close_rows as (
  select
    t.sk_accountid, t.accountid, t.sk_brokerid, t.sk_customerid, t.accountdesc,
    t.taxstatus, t.status, false as iscurrent, t.batchid, t.effectivedate,
    n.effectivedate as enddate
  from {{ this }} t
  join (select distinct accountid, effectivedate from new_rows) n on t.accountid = n.accountid
  where t.iscurrent
)
select * from new_rows
union all select * from close_rows

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
