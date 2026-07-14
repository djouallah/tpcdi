-- Alert messages. Ported from shannon-barrow/databricks-tpc-di
--   src/incremental_batches/audit_validation/audit_alerts.sql
-- Emits MessageType='Alert' rows into DImessages for records that trip a data-quality
-- rule. The audit's 'DimCustomer age range alerts', 'DimCustomer customer tier alerts',
-- 'DimTrade commission alerts' and 'DimTrade charge alerts' checks count these against the
-- PDGF Audit answer keys. (The 'Invalid SPRating' and 'No earnings for company' alerts are
-- emitted too, for fidelity, though the audit does not count them.) run.py runs this once,
-- after batch 3, before the audit.
-- Dialect: bare table names (schema on connection); CURRENT_TIMESTAMP() -> now()::timestamp;
--   nvl -> coalesce; cast(... AS string) -> cast(... AS varchar); datediff(YEAR,a,b) ->
--   datediff('year',a,b); _stage.finwire -> bare finwire, and the CIK/SPRating substring
--   offsets shifted by -18 to match this project's FinWire.value = substr(line, 19)
--   (classic value is the full line: CIK 79->61, SPRating 95->77).
INSERT INTO DImessages
SELECT
  now()::timestamp AS MessageDateAndTime,
  batchid,
  MessageSource,
  MessageText,
  'Alert' AS MessageType,
  MessageData
FROM (
  SELECT
    batchid,
    'DimCustomer' AS MessageSource,
    'Invalid customer tier' AS MessageText,
    concat('C_ID = ', customerid, ', C_TIER = ', coalesce(cast(tier AS varchar), 'null')) AS MessageData
  FROM (
    SELECT customerid, tier, batchid
    FROM DimCustomer
    WHERE tier NOT IN (1, 2, 3) OR tier IS NULL
    QUALIFY row_number() OVER (PARTITION BY customerid, batchid ORDER BY enddate DESC) = 1)
  UNION ALL
  SELECT DISTINCT
    batchid,
    'DimCustomer',
    'DOB out of range',
    concat('C_ID = ', customerid, ', C_DOB = ', dob)
  FROM DimCustomer dc
  JOIN batchdate bd USING (batchid)
  -- "100+ years old" = has actually reached the 100th birthday. Databricks
  -- datediff(YEAR, dob, batchdate) counts FULL years elapsed, but DuckDB
  -- datediff('year', ...) counts the CALENDAR-year difference (year(b) - year(a)),
  -- which over-flags anyone born in batch_year-100 whose birthday falls after the
  -- batch date (they're really 99). The birthday-accurate test is dob <= batchdate - 100y.
  WHERE dob <= batchdate - INTERVAL 100 YEAR OR dob > batchdate
  UNION ALL
  SELECT DISTINCT
    batchid,
    'DimTrade',
    'Invalid trade commission',
    concat('T_ID = ', tradeid, ', T_COMM = ', commission)
  FROM DimTrade
  WHERE commission IS NOT NULL AND commission > tradeprice * quantity
  UNION ALL
  SELECT DISTINCT
    batchid,
    'DimTrade',
    'Invalid trade fee',
    concat('T_ID = ', tradeid, ', T_CHRG = ', fee)
  FROM DimTrade
  WHERE fee IS NOT NULL AND fee > tradeprice * quantity
  UNION ALL
  SELECT DISTINCT
    fmh.batchid,
    'FactMarketHistory',
    'No earnings for company',
    concat('DM_S_SYMB = ', symbol)
  FROM FactMarketHistory fmh
  JOIN DimSecurity ds ON ds.sk_securityid = fmh.sk_securityid
  WHERE peratio IS NULL
  UNION ALL
  SELECT DISTINCT
    1 AS batchid,
    'DimCompany',
    'Invalid SPRating',
    concat('CO_ID = ', cik, ', CO_SP_RATE = ', sprating)
  FROM (
    SELECT trim(substr(value, 61, 10)) AS cik,
           trim(substr(value, 77, 4))  AS sprating
    FROM finwire
    WHERE rectype = 'CMP')
  WHERE sprating NOT IN ('AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+','AA-','A-','BBB-','BB-','B-','CCC-')
);
