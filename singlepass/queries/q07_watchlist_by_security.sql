-- Watch-list activity per security: total, still-active, and cancelled watches.
-- A watch is still active when it has no removal date (sk_dateid_dateremoved is null).
select
  s.symbol,
  s.name,
  count(*)                                                     as watches,
  count(*) filter (where w.sk_dateid_dateremoved is null)     as active_watches,
  count(*) filter (where w.sk_dateid_dateremoved is not null) as cancelled_watches
from "FactWatches" w
join "DimSecurity" s on s.sk_securityid = w.sk_securityid
group by s.symbol, s.name
order by watches desc
limit 50;
