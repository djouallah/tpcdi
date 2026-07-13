-- Trade count, share volume and value by trade type.
-- DimTrade already carries the long-form trade type, so no TradeType join is needed.
select
  t.type,
  count(*)                       as trades,
  sum(t.quantity)                as shares,
  sum(t.tradeprice * t.quantity) as gross_value,
  sum(t.commission)              as commission
from "DimTrade" t
group by t.type
order by trades desc;
