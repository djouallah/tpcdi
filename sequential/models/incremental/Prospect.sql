{{ config(materialized='table') }}
-- Prospect (current prospect list, as-of this batch). Sequential (3-batch) port.
-- Ported from silver/Prospect.sql — the classic does a full INSERT OVERWRITE per batch
-- ("only 3 total days"), so this is a `table` rebuilt every batch (tag:incremental), not a
-- merge. Reads bronzeprospect as-of the batch and flags iscustomer against the CURRENT
-- DimCustomer (built earlier this batch). sk_recorddateid >= sk_updatedateid by
-- construction (recordbatchid = last batch seen, capped at this batch; batchid = first).
{% set b = var('batch') %}
with p as (
  select
    * exclude (recordbatchid),
    least(recordbatchid, {{ b }}) as recordbatchid
  from {{ ref('bronzeprospect') }}
  where batchid <= {{ b }} and recordbatchid >= {{ b }}
),
cust as (
  select lastname, firstname, addressline1, addressline2, postalcode
  from {{ ref('DimCustomer') }}
  where iscurrent
)
select
  p.agencyid,
  cast(strftime(recdate.batchdate, '%Y%m%d') as bigint) as sk_recorddateid,
  cast(strftime(origdate.batchdate, '%Y%m%d') as bigint) as sk_updatedateid,
  p.batchid,
  (c.lastname is not null) as iscustomer,
  p.lastname, p.firstname, p.middleinitial, p.gender,
  p.addressline1, p.addressline2, p.postalcode, p.city, p.state, p.country, p.phone,
  p.income, p.numbercars, p.numberchildren, p.maritalstatus, p.age, p.creditrating,
  p.ownorrentflag, p.employer, p.numbercreditcards, p.networth, p.marketingnameplate
from p
join {{ ref('BatchDate') }} recdate on p.recordbatchid = recdate.batchid
join {{ ref('BatchDate') }} origdate on p.batchid = origdate.batchid
left join cust c
  on upper(p.lastname) = upper(c.lastname)
 and upper(p.firstname) = upper(c.firstname)
 and upper(p.addressline1) = upper(c.addressline1)
 and upper(coalesce(p.addressline2, '')) = upper(coalesce(c.addressline2, ''))
 and upper(p.postalcode) = upper(c.postalcode)
