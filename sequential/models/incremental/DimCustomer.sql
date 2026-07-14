{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sk_customerid',
    merge_update_columns=['iscurrent', 'enddate', 'agencyid', 'creditrating', 'networth', 'marketingnameplate'],
) }}
-- depends_on: {{ ref('BatchDate') }}
-- (BatchDate is ref'd only inside the is_incremental() branch, so dbt can't infer the
--  dep at parse time — is_incremental() is False during parsing. This hint declares it.)
-- DimCustomer, SCD2. Sequential (3-batch) port.
--   Historical branch (batch 1): silver/DimCustomer Historical.sql
--   Incremental branch (batches 2-3): silver/DimCustomer Incremental.sql
-- Gender normalized (upper). BIGINT surrogate keys (yyyyMMdd || customerid).
--
-- The classic uses a native MERGE with two WHEN MATCHED clauses (close old version;
-- prospect-only update) — different SET lists, which one dbt/duckrun merge can't express.
-- Reshaped into ONE merge whose source is new_rows + close_rows + prospect_update_rows,
-- with merge_update_columns covering BOTH clauses' columns and the three sets DISJOINT by
-- sk_customerid so every target row is matched by exactly one source row:
--   new_rows            -> new sk        -> NOT MATCHED -> INSERT
--   close_rows          -> old sk        -> MATCHED     -> set iscurrent=false, enddate
--   prospect_update_rows-> current sk    -> MATCHED     -> set agencyid/creditrating/...
-- Each source row carries the correct value for ALL six update columns (unchanged ones
-- carry their existing value), so a match applies exactly the intended change.
{% set b = var('batch') %}
{% if is_incremental() %}

-- Incremental (batch N): new customer versions come from Batch N Customer.txt (every row
-- is a change). Prospect enrichment is re-evaluated as of this batch.
with cust_cdc as (
  select
    customerid,
    nullif(taxid, '') as taxid,
    {{ status_longform('status') }} as status,
    nullif(lastname, '') as lastname,
    nullif(firstname, '') as firstname,
    nullif(middleinitial, '') as middleinitial,
    nullif(gender, '') as gender,
    tier, dob,
    nullif(addressline1, '') as addressline1,
    nullif(addressline2, '') as addressline2,
    nullif(postalcode, '') as postalcode,
    nullif(city, '') as city,
    nullif(stateprov, '') as stateprov,
    country,
    {{ cm_phone('c_ctry_1', 'c_area_1', 'c_local_1', 'c_ext_1') }} as phone1,
    {{ cm_phone('c_ctry_2', 'c_area_2', 'c_local_2', 'c_ext_2') }} as phone2,
    {{ cm_phone('c_ctry_3', 'c_area_3', 'c_local_3', 'c_ext_3') }} as phone3,
    nullif(email1, '') as email1,
    nullif(email2, '') as email2,
    nullif(lcl_tx_id, '') as lcl_tx_id,
    nullif(nat_tx_id, '') as nat_tx_id
  from {{ read_pipe('Batch' ~ b ~ '/Customer.txt',
    "{'cdc_flag': 'VARCHAR', 'cdc_dsn': 'BIGINT', 'customerid': 'BIGINT',
      'taxid': 'VARCHAR', 'status': 'VARCHAR', 'lastname': 'VARCHAR', 'firstname': 'VARCHAR',
      'middleinitial': 'VARCHAR', 'gender': 'VARCHAR', 'tier': 'TINYINT', 'dob': 'DATE',
      'addressline1': 'VARCHAR', 'addressline2': 'VARCHAR', 'postalcode': 'VARCHAR',
      'city': 'VARCHAR', 'stateprov': 'VARCHAR', 'country': 'VARCHAR',
      'c_ctry_1': 'VARCHAR', 'c_area_1': 'VARCHAR', 'c_local_1': 'VARCHAR', 'c_ext_1': 'VARCHAR',
      'c_ctry_2': 'VARCHAR', 'c_area_2': 'VARCHAR', 'c_local_2': 'VARCHAR', 'c_ext_2': 'VARCHAR',
      'c_ctry_3': 'VARCHAR', 'c_area_3': 'VARCHAR', 'c_local_3': 'VARCHAR', 'c_ext_3': 'VARCHAR',
      'email1': 'VARCHAR', 'email2': 'VARCHAR', 'lcl_tx_id': 'VARCHAR', 'nat_tx_id': 'VARCHAR'}") }}
),
bd as (select batchdate from {{ ref('BatchDate') }} where batchid = {{ b }}),
new_rows as (
  select
    cast(concat(strftime((select batchdate from bd), '%Y%m%d'), cast(c.customerid as varchar)) as bigint) as sk_customerid,
    c.customerid, c.taxid, c.status, c.lastname, c.firstname, c.middleinitial,
    case when upper(c.gender) in ('M', 'F') then upper(c.gender) else 'U' end as gender,
    c.tier, c.dob, c.addressline1, c.addressline2, c.postalcode, c.city, c.stateprov, c.country,
    c.phone1, c.phone2, c.phone3, c.email1, c.email2,
    r_nat.tx_name as nationaltaxratedesc, r_nat.tx_rate as nationaltaxrate,
    r_lcl.tx_name as localtaxratedesc, r_lcl.tx_rate as localtaxrate,
    p.agencyid, p.creditrating, p.networth, p.marketingnameplate,
    true as iscurrent, {{ b }} as batchid,
    (select batchdate from bd) as effectivedate, date '9999-12-31' as enddate
  from cust_cdc c
  join {{ ref('TaxRate') }} r_lcl on c.lcl_tx_id = r_lcl.tx_id
  join {{ ref('TaxRate') }} r_nat on c.nat_tx_id = r_nat.tx_id
  left join {{ ref('bronzeprospect') }} p
    on p.batchid <= {{ b }} and p.recordbatchid >= {{ b }}
   and upper(p.lastname) = upper(c.lastname)
   and upper(p.firstname) = upper(c.firstname)
   and upper(p.addressline1) = upper(c.addressline1)
   and upper(coalesce(p.addressline2, '')) = upper(coalesce(c.addressline2, ''))
   and upper(p.postalcode) = upper(c.postalcode)
),
-- Close the current version of every customer that got a new version this batch.
close_rows as (
  select
    t.sk_customerid, t.customerid, t.taxid, t.status, t.lastname, t.firstname, t.middleinitial,
    t.gender, t.tier, t.dob, t.addressline1, t.addressline2, t.postalcode, t.city, t.stateprov, t.country,
    t.phone1, t.phone2, t.phone3, t.email1, t.email2,
    t.nationaltaxratedesc, t.nationaltaxrate, t.localtaxratedesc, t.localtaxrate,
    t.agencyid, t.creditrating, t.networth, t.marketingnameplate,
    false as iscurrent, t.batchid, t.effectivedate,
    n.effectivedate as enddate
  from {{ this }} t
  join (select distinct customerid, effectivedate from new_rows) n on t.customerid = n.customerid
  where t.iscurrent
),
-- Prospects added (batchid = this batch) or removed (last seen the prior batch) this batch.
prospect_changed as (
  select agencyid, creditrating, networth, marketingnameplate,
         lastname, firstname, addressline1, addressline2, postalcode, batchid
  from {{ ref('bronzeprospect') }}
  where batchid = {{ b }} or (recordbatchid = {{ b }} - 1 and batchid < {{ b }})
  qualify row_number() over (partition by agencyid order by batchid desc) = 1
),
-- Prospect-only updates: current customers with NO new version this batch whose matching
-- prospect changed. New prospect -> set enrichment; removed prospect -> clear it.
prospect_update_rows as (
  select
    t.sk_customerid, t.customerid, t.taxid, t.status, t.lastname, t.firstname, t.middleinitial,
    t.gender, t.tier, t.dob, t.addressline1, t.addressline2, t.postalcode, t.city, t.stateprov, t.country,
    t.phone1, t.phone2, t.phone3, t.email1, t.email2,
    t.nationaltaxratedesc, t.nationaltaxrate, t.localtaxratedesc, t.localtaxrate,
    case when pc.batchid = {{ b }} then pc.agencyid end as agencyid,
    case when pc.batchid = {{ b }} then pc.creditrating end as creditrating,
    case when pc.batchid = {{ b }} then pc.networth end as networth,
    case when pc.batchid = {{ b }} then pc.marketingnameplate end as marketingnameplate,
    t.iscurrent, t.batchid, t.effectivedate, t.enddate
  from {{ this }} t
  join prospect_changed pc
    on upper(pc.lastname) = upper(t.lastname)
   and upper(pc.firstname) = upper(t.firstname)
   and upper(pc.addressline1) = upper(t.addressline1)
   and upper(coalesce(pc.addressline2, '')) = upper(coalesce(t.addressline2, ''))
   and upper(pc.postalcode) = upper(t.postalcode)
  where t.iscurrent
    and not exists (select 1 from new_rows n where n.customerid = t.customerid)
)
select * from new_rows
union all select * from close_rows
union all select * from prospect_update_rows

{% else %}

-- Historical load (batch 1): versions come only from CustomerMgmt.xml (stg_customermgmt).
with customers as (
  select
    customerid, taxid, status, lastname, firstname, middleinitial, gender, tier,
    dob, addressline1, addressline2, postalcode, city, stateprov, country,
    phone1, phone2, phone3, email1, email2, lcl_tx_id, nat_tx_id, update_ts
  from {{ ref('stg_customermgmt') }}
  where actiontype in ('NEW', 'INACT', 'UPDCUST')
),
{% set ff = "over (partition by customerid order by update_ts rows between unbounded preceding and current row)" %}
customerfinal as (
  select
    customerid,
    coalesce(taxid,         last_value(taxid         ignore nulls) {{ ff }}) as taxid,
    status,
    coalesce(lastname,      last_value(lastname      ignore nulls) {{ ff }}) as lastname,
    coalesce(firstname,     last_value(firstname     ignore nulls) {{ ff }}) as firstname,
    coalesce(middleinitial, last_value(middleinitial ignore nulls) {{ ff }}) as middleinitial,
    coalesce(gender,        last_value(gender        ignore nulls) {{ ff }}) as gender,
    coalesce(tier,          last_value(tier          ignore nulls) {{ ff }}) as tier,
    coalesce(dob,           last_value(dob           ignore nulls) {{ ff }}) as dob,
    coalesce(addressline1,  last_value(addressline1  ignore nulls) {{ ff }}) as addressline1,
    coalesce(addressline2,  last_value(addressline2  ignore nulls) {{ ff }}) as addressline2,
    coalesce(postalcode,    last_value(postalcode    ignore nulls) {{ ff }}) as postalcode,
    coalesce(city,          last_value(city          ignore nulls) {{ ff }}) as city,
    coalesce(stateprov,     last_value(stateprov     ignore nulls) {{ ff }}) as stateprov,
    coalesce(country,       last_value(country       ignore nulls) {{ ff }}) as country,
    coalesce(phone1,        last_value(phone1        ignore nulls) {{ ff }}) as phone1,
    coalesce(phone2,        last_value(phone2        ignore nulls) {{ ff }}) as phone2,
    coalesce(phone3,        last_value(phone3        ignore nulls) {{ ff }}) as phone3,
    coalesce(email1,        last_value(email1        ignore nulls) {{ ff }}) as email1,
    coalesce(email2,        last_value(email2        ignore nulls) {{ ff }}) as email2,
    coalesce(lcl_tx_id,     last_value(lcl_tx_id     ignore nulls) {{ ff }}) as lcl_tx_id,
    coalesce(nat_tx_id,     last_value(nat_tx_id     ignore nulls) {{ ff }}) as nat_tx_id,
    lead(update_ts) over (partition by customerid order by update_ts) is null as iscurrent,
    date(update_ts) as effectivedate,
    coalesce(
      lead(date(update_ts)) over (partition by customerid order by update_ts),
      date '9999-12-31') as enddate,
    1 as batchid
  from customers
)
select
  cast(concat(strftime(c.effectivedate, '%Y%m%d'), cast(c.customerid as varchar)) as bigint) as sk_customerid,
  c.customerid, c.taxid, c.status, c.lastname, c.firstname, c.middleinitial,
  case when c.gender in ('M', 'F') then c.gender else 'U' end as gender,
  c.tier, c.dob, c.addressline1, c.addressline2, c.postalcode, c.city,
  c.stateprov, c.country, c.phone1, c.phone2, c.phone3, c.email1, c.email2,
  r_nat.tx_name as nationaltaxratedesc,
  r_nat.tx_rate as nationaltaxrate,
  r_lcl.tx_name as localtaxratedesc,
  r_lcl.tx_rate as localtaxrate,
  p.agencyid, p.creditrating, p.networth, p.marketingnameplate,
  c.iscurrent, c.batchid, c.effectivedate, c.enddate
from customerfinal c
join {{ ref('TaxRate') }} r_lcl on c.lcl_tx_id = r_lcl.tx_id
join {{ ref('TaxRate') }} r_nat on c.nat_tx_id = r_nat.tx_id
left join {{ ref('bronzeprospect') }} p
  on p.batchid = 1
 and upper(p.lastname) = upper(c.lastname)
 and upper(p.firstname) = upper(c.firstname)
 and upper(p.addressline1) = upper(c.addressline1)
 and upper(coalesce(p.addressline2, '')) = upper(coalesce(c.addressline2, ''))
 and upper(p.postalcode) = upper(c.postalcode)
where c.effectivedate < c.enddate

{% endif %}
