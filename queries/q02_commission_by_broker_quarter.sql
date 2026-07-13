-- Trade activity, commission and fees by broker branch and calendar quarter.
-- Joins DimTrade to DimBroker (sk_brokerid) and to DimDate on the trade CREATE date.
select
  b.branch,
  d.calendaryearid,
  d.calendarqtrid,
  count(*)                        as trades,
  sum(t.commission)              as commission,
  sum(t.fee)                     as fees,
  sum(t.tradeprice * t.quantity) as gross_value
from "DimTrade" t
join "DimBroker" b on b.sk_brokerid = t.sk_brokerid
join "DimDate"   d on d.sk_dateid   = t.sk_createdateid
group by b.branch, d.calendaryearid, d.calendarqtrid
order by b.branch, d.calendaryearid, d.calendarqtrid;
