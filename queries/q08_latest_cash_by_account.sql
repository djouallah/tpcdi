-- Latest cash balance per account (top 50 by balance).
-- FactCashBalances holds a running balance per account per day; take the newest day.
select
  sk_accountid,
  sk_dateid as as_of_dateid,
  cash      as latest_cash
from "FactCashBalances"
qualify row_number() over (partition by sk_accountid order by sk_dateid desc) = 1
order by latest_cash desc
limit 50;
