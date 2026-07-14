-- Revenue and EPS aggregates by industry.
-- Financial is per company per quarter; join to the company version via sk_companyid.
-- DimCompany already carries the industry name, so no Industry join is needed.
select
  c.industry,
  count(distinct c.companyid) as companies,
  sum(f.fi_revenue)           as total_revenue,
  avg(f.fi_basic_eps)         as avg_basic_eps,
  avg(f.fi_net_earn)          as avg_net_earnings
from "Financial" f
join "DimCompany" c on c.sk_companyid = f.sk_companyid
group by c.industry
order by total_revenue desc;
