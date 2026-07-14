-- Top 50 held securities by current market value.
-- Latest holding per (account, security) so positions aren't double-counted; the fact
-- pins the security version via sk_securityid, so join on that key (no iscurrent filter).
with latest_holding as (
  select sk_securityid, sk_accountid, currentholding, currentprice
  from "FactHoldings"
  qualify row_number() over (
    partition by sk_accountid, sk_securityid
    order by sk_dateid desc, currenttradeid desc
  ) = 1
)
select
  s.symbol,
  s.name,
  s.exchangeid,
  sum(h.currentholding)                   as shares_held,
  sum(h.currentholding * h.currentprice)  as market_value
from latest_holding h
join "DimSecurity" s on s.sk_securityid = h.sk_securityid
group by s.symbol, s.name, s.exchangeid
order by market_value desc
limit 50;
