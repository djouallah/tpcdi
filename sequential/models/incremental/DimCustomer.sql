{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sk_customerid',
    merge_update_columns=['iscurrent', 'enddate'],
) }}
-- DimCustomer, SCD2. Sequential (3-batch) port.
--   Historical branch (batch 1): silver/DimCustomer Historical.sql
--   Incremental branch (batches 2-3): silver/DimCustomer Incremental.sql  [Stage 5]
-- Gender is normalized in stg_customermgmt (nullif(upper(gender),'')). Surrogate keys are
-- BIGINT (yyyyMMdd || customerid) per the spec schema so cross-table sk joins and the
-- SCD2 merge stay type-consistent.
{% if is_incremental() %}

-- Incremental merge branch implemented in Stage 5. No-op placeholder until then.
select * from {{ this }} limit 0

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
