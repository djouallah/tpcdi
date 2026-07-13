-- Monthly average close price and total volume per security.
-- FactMarketHistory -> DimSecurity (sk_securityid) and DimDate (sk_dateid).
select
  s.symbol,
  d.calendaryearid,
  d.calendarmonthid,
  avg(m.closeprice) as avg_close,
  sum(m.volume)     as total_volume,
  max(m.dayhigh)    as month_high,
  min(m.daylow)     as month_low
from "FactMarketHistory" m
join "DimSecurity" s on s.sk_securityid = m.sk_securityid
join "DimDate"     d on d.sk_dateid     = m.sk_dateid
group by s.symbol, d.calendaryearid, d.calendarmonthid
order by s.symbol, d.calendaryearid, d.calendarmonthid;
