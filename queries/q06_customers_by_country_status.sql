-- Current customer counts by country and status.
-- Standalone dimension query: filter to the current SCD2 version of each customer.
select
  country,
  status,
  count(*) as customers
from "DimCustomer"
where iscurrent
group by country, status
order by country, status;
