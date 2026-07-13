-- Current portfolio market value and account count by customer tier.
-- FactHoldings is at holding-history grain, so take the latest holding per
-- (account, security) to avoid double-counting positions. The fact pins a specific
-- SCD2 customer version via sk_customerid, so we join on that key directly (no
-- iscurrent filter, which would wrongly drop holdings that point at an older version).
with latest_holding as (
  select sk_customerid, sk_accountid, sk_securityid, currentholding, currentprice
  from "FactHoldings"
  qualify row_number() over (
    partition by sk_accountid, sk_securityid
    order by sk_dateid desc, currenttradeid desc
  ) = 1
)
select
  c.tier,
  count(distinct h.sk_accountid)          as accounts,
  sum(h.currentholding * h.currentprice)  as market_value
from latest_holding h
join "DimCustomer" c on c.sk_customerid = h.sk_customerid
group by c.tier
order by c.tier;
