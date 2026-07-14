{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}
-- FactMarketHistory (daily market with P/E, yield, trailing 52-week high/low).
-- Sequential (3-batch) port.
--   Historical branch (batch 1): gold/FactMarketHistory Historical.sql
--   Incremental branch (batches 2-3): gold/FactMarketHistory Incremental.sql  [Stage 7]
-- The 52-week high/low are precomputed in bronzedailymarket (min_by/max_by over a trailing
-- 364-row window ordered by date — the carried fix). P/E uses the trailing-4-quarter EPS
-- sum; Financial is batch-1 static.
{% if is_incremental() %}

-- Incremental (batch N): append this batch's rows from the cumulative bronze. Per classic
-- the security join is on iscurrent (recent dates -> current security version).
{% set b = var('batch') %}
with companyfinancials as (
  select
    f.sk_companyid, f.fi_qtr_start_date,
    sum(f.fi_basic_eps) over (
      partition by d.companyid order by f.fi_qtr_start_date
      rows between 4 preceding and 1 preceding) as sum_fi_basic_eps
  from {{ ref('Financial') }} f
  join {{ ref('DimCompany') }} d on f.sk_companyid = d.sk_companyid
)
select
  s.sk_securityid,
  s.sk_companyid,
  cast(strftime(fmh.dm_date, '%Y%m%d') as bigint) as sk_dateid,
  fmh.dm_close / nullif(f.sum_fi_basic_eps, 0) as peratio,
  (s.dividend / nullif(fmh.dm_close, 0)) / 100 as yield,
  fmh.fiftytwoweekhigh, fmh.sk_fiftytwoweekhighdate,
  fmh.fiftytwoweeklow, fmh.sk_fiftytwoweeklowdate,
  fmh.dm_close as closeprice, fmh.dm_high as dayhigh, fmh.dm_low as daylow,
  fmh.dm_vol as volume, fmh.batchid
from {{ ref('bronzedailymarket') }} fmh
join {{ ref('DimSecurity') }} s on s.symbol = fmh.dm_s_symb and s.iscurrent
left join companyfinancials f
  on f.sk_companyid = s.sk_companyid
 and extract(quarter from fmh.dm_date) = extract(quarter from f.fi_qtr_start_date)
 and extract(year from fmh.dm_date) = extract(year from f.fi_qtr_start_date)
where fmh.batchid = {{ b }}

{% else %}

-- Historical load (batch 1).
with companyfinancials as (
  select
    f.sk_companyid,
    f.fi_qtr_start_date,
    sum(f.fi_basic_eps) over (
      partition by d.companyid order by f.fi_qtr_start_date
      rows between 4 preceding and 1 preceding
    ) as sum_fi_basic_eps
  from {{ ref('Financial') }} f
  join {{ ref('DimCompany') }} d on f.sk_companyid = d.sk_companyid
)
select
  s.sk_securityid,
  s.sk_companyid,
  cast(strftime(fmh.dm_date, '%Y%m%d') as bigint) as sk_dateid,
  fmh.dm_close / nullif(f.sum_fi_basic_eps, 0) as peratio,
  (s.dividend / nullif(fmh.dm_close, 0)) / 100 as yield,
  fmh.fiftytwoweekhigh,
  fmh.sk_fiftytwoweekhighdate,
  fmh.fiftytwoweeklow,
  fmh.sk_fiftytwoweeklowdate,
  fmh.dm_close as closeprice,
  fmh.dm_high as dayhigh,
  fmh.dm_low as daylow,
  fmh.dm_vol as volume,
  fmh.batchid
from {{ ref('bronzedailymarket') }} fmh
join {{ ref('DimSecurity') }} s
  on s.symbol = fmh.dm_s_symb
 and fmh.dm_date >= s.effectivedate
 and fmh.dm_date < s.enddate
left join companyfinancials f
  on f.sk_companyid = s.sk_companyid
 and extract(quarter from fmh.dm_date) = extract(quarter from f.fi_qtr_start_date)
 and extract(year from fmh.dm_date) = extract(year from f.fi_qtr_start_date)
where fmh.batchid = 1

{% endif %}
